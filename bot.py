import discord
from discord.ext import commands, tasks

import subprocess
import argparse
import yaml
import logging

import asyncio
import gspread_asyncio

from google.oauth2.service_account import Credentials


# Parse the command line arguments
parser = argparse.ArgumentParser(description="Run MT Gardener")
parser.add_argument(
    "--config", type=str, required=True, help="The path to the configuration yaml file."
)
args = parser.parse_args()


# Load the configuration file
config = {}
with open(args.config, "r") as f:
    config = yaml.safe_load(f)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(
            config["logging_path"] if "logging_path" in config else "bot.log"
        ),
        logging.StreamHandler(),
    ],
)

logging.info("Loading configuration...")
BOT_TOKEN = config["bot_token"]
MT_SERVER_ID = config["server_id"]
FEEDBACK_CHANNEL_ID = config["feedback_channel_id"]
ATTENDANCE_CHANNEL_ID = config["attendance_channel_id"]
ALERT_CHANNEL_ID = config["alert_channel_id"]
ALERT_MESSAGE_ID = config["alert_message_id"]

intents = discord.Intents.default()
intents.messages = True
intents.message_content = True
intents.members = True
intents.reactions = True

bot = commands.Bot(command_prefix="!", case_insensitive=True, intents=intents)
bot.description = """MT Gardener is Mother Tree's little personal assistant bot.

It does little things to make life a little easier (hopefully) on the folks who wish to use it.
To use it, send the bot a DM with a command, like `!changelog` or `!help`.

Reach out to Barumaru with any feedback!"""

SUGGESTION_TEMPLATE = """
**I've got a new suggestion to pass on!**

>>> {}
"""
SUGGESTION_LENGTH_MINIMUM = 20

logging.info("Loading Google Sheets integration...")
GOOGLE_CREDS_JSON = config["google_service_account_creds"]
GOOGLE_SHEETS_URL = config["google_sheets_url"]
JOB_SHEET_URL = config["job_sheet_url"]


def get_creds():
    creds = Credentials.from_service_account_file(GOOGLE_CREDS_JSON)
    scoped = creds.with_scopes(
        [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
    )
    return scoped


agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)

PROBOT_ID = int(config["probot_id"])

LAST_POLL_MESSAGE = None


# Custom check functions that can disallow commands from being run
async def check_channel_is_dm(ctx):
    return isinstance(ctx.channel, discord.channel.DMChannel)


async def check_user_is_council_or_dev(ctx):
    guild = bot.get_guild(MT_SERVER_ID)
    user = ctx.message.author
    member = discord.utils.find(lambda m: m.id == user.id, guild.members)
    role = discord.utils.find(
        lambda r: r.name in ("Elder Tree Council", "MT Gardener Dev"), member.roles
    )
    return bool(role)


# Error handler
@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.errors.CheckFailure):
        pass


@bot.command()
@commands.check(check_channel_is_dm)
async def suggest(ctx):
    if len(ctx.message.content.split(" ")) < 2:
        return await ctx.send(
            "Usage example: `!suggest I think that Barumaru should get all the loot from now on!`"
        )
    elif len(ctx.message.content) < SUGGESTION_LENGTH_MINIMUM:
        return await ctx.send(
            "Please elaborate a little bit more with your suggestion."
        )
    channel = discord.utils.get(bot.get_all_channels(), id=FEEDBACK_CHANNEL_ID)
    content = ctx.message.content[len("!suggest ") :]
    suggestion_message = await channel.send(SUGGESTION_TEMPLATE.format(content))
    thread = await suggestion_message.create_thread(name="Suggestion Feedback")
    await thread.send(
        f"*Feel free to leave feedback and discuss this suggestion here. Please be civil!*"
    )
    await ctx.send(
        f"I've passed along your suggestion. You can read the discussion here: {suggestion_message.jump_url}"
    )


async def update_att_sheet(last_poll_message):
    logging.info("Updating poll responses...")
    reaction_map = {}
    for reaction in last_poll_message.reactions:
        reaction_map[reaction.emoji.name] = []
        async for user in reaction.users():
            if user.id == PROBOT_ID:
                continue
            reaction_map[reaction.emoji.name].append(
                f"{user.name}#{user.discriminator}"
            )

    agc = await agcm.authorize()
    ss = await agc.open_by_url(GOOGLE_SHEETS_URL)
    ws = await ss.worksheet("Linkshell Roster")

    user_id_col_values = await ws.col_values(2)

    # determine the window of actual user names in the column, we need it to figure out non-responses
    start_index = user_id_col_values.index("Discord Tag") + 1
    end_index = user_id_col_values.index("", 6) - 1

    full_roster = set(user_id_col_values[start_index : end_index + 1])

    current_att_col_values = await ws.col_values(23)
    current_att_col_values.extend(
        ["" for _ in range(len(user_id_col_values) - len(current_att_col_values))]
    )

    def get_concat_list_for_keys(reaction_map, keys):
        ret = []
        for key in keys:
            if key not in reaction_map:
                continue
            ret.extend(reaction_map[key])
        return ret

    update_map = {}
    for user_id in get_concat_list_for_keys(reaction_map, ["attcheck", "attcheck2"]):
        update_map[user_id] = "o"

    for user_id in get_concat_list_for_keys(
        reaction_map, ["attearly", "attlate", "attmaybe"]
    ):
        update_map[user_id] = "/"

    for user_id in get_concat_list_for_keys(reaction_map, ["attdecline"]):
        update_map[user_id] = "x"

    for user_id in full_roster - set(update_map.keys()):
        update_map[user_id] = ""

    # Generate batch-update commands for each user so we don't get rate-limited
    # Fields that are already the right value no-op and are not added to batch update
    def add_to_batch(
        batch_updates, user_id, user_id_col_values, current_att_col_values, val
    ):
        try:
            index = user_id_col_values.index(user_id)
            current = current_att_col_values[index]
            if current == val:
                return

            logging.info(f"Updating att poll response for {user_id}")
            batch_updates.append({"range": f"W{index + 1}", "values": [[val]]})
        except Exception as e:
            logging.info(e)

    batch_updates = []
    for key in update_map:
        add_to_batch(
            batch_updates,
            key,
            user_id_col_values,
            current_att_col_values,
            update_map[key],
        )

    await ws.batch_update(batch_updates)


@bot.command()
@commands.check(check_channel_is_dm)
async def job(ctx):
    msgs = await _job([ctx.message.author])
    if ctx.author in msgs:
        await ctx.author.send(msgs[ctx.author])


async def _job(users):
    agc = await agcm.authorize()
    roster_ss = await agc.open_by_url(GOOGLE_SHEETS_URL)
    roster_ws = await roster_ss.worksheet("Linkshell Roster")
    col_values = await roster_ws.col_values(2)

    party_ss = await agc.open_by_url(JOB_SHEET_URL)
    party_ws = await party_ss.worksheet("Party Setup")

    row_indexes = {}
    for user in users:
        account_id = f"{user.name}#{user.discriminator}"
        logging.info("  Cross locating " + account_id)
        row_indexes[user.id] = [i for i, x in enumerate(col_values) if x == account_id][
            :2
        ]

    msgs = {}
    for user in users:
        logging.info(f"  Handling user {user}")
        if not row_indexes[user.id]:
            logging.info("    Couldn't located account on sheet, skipping...")
            continue

        user_row_indexes = row_indexes[user.id]
        character_name_cell = await roster_ws.acell(f"A{user_row_indexes[0] + 1}")
        character_name = character_name_cell.value

        alt_name = None
        if len(user_row_indexes) > 1:
            alt_name_cell = await roster_ws.acell(f"A{user_row_indexes[1] + 1}")
            alt_name = alt_name_cell.value

        col_values = await party_ws.col_values(2)
        try:
            row = col_values.index(character_name) + 1
            job_cell = await party_ws.acell(f"C{row}")
            job = job_cell.value

            alt_assigned = False
            if alt_name:
                try:
                    alt_row = col_values.index(alt_name) + 1
                    alt_job_cell = await party_ws.acell(f"C{alt_row}")
                    alt_job = alt_job_cell.value
                    alt_assigned = True
                except:
                    pass

            msg_main = f"[{character_name}: **{job}**]"
            msg_sub = ""
            if alt_assigned:
                msg_sub = f"[{alt_name}: **{alt_job}**]"

            msgs[user] = f"{user.mention} - {msg_main} {msg_sub}"
        except:
            msgs[user] = f"{user.mention} - You're not on the job sheet."

    return msgs


@bot.command()
async def ping(ctx):
    return await ctx.send("Pong!")


@bot.command()
@commands.check(check_channel_is_dm)
async def changelog(ctx):
    num_commits = 3
    version_content = subprocess.check_output(
        ["git", "log", "--use-mailmap", f"-n{num_commits}"]
    )
    await ctx.send("Most recent changes:\n```" + str(version_content, "utf-8") + "```")


@bot.command()
@commands.check(check_channel_is_dm)
@commands.check(check_user_is_council_or_dev)
async def attupdate(ctx):
    channel = discord.utils.get(bot.get_all_channels(), id=ATTENDANCE_CHANNEL_ID)
    last_poll_message = None
    async for message in channel.history(limit=10):
        if message.author.id == int(PROBOT_ID):
            last_poll_message = message
            break
    await update_att_sheet(last_poll_message)


@bot.command()
@commands.check(check_channel_is_dm)
@commands.check(check_user_is_council_or_dev)
async def alertjobs(ctx):
    update_msg = "*Grabbing users to alert...* "
    message = await ctx.send(update_msg)

    alert_channel = discord.utils.get(bot.get_all_channels(), id=ALERT_CHANNEL_ID)
    sub_message = await alert_channel.fetch_message(ALERT_MESSAGE_ID)
    reaction = discord.utils.get(sub_message.reactions, emoji="📣")
    update_msg += "**Done**\n*Fetching users' jobs...* "
    await message.edit(content=update_msg)

    def get_user_alert_section(alerted_users):
        user_alert_section = "```"
        for user in alerted_users:
            user_alert_section += f"{user.name}#{user.discriminator} - {'DONE' if alerted_users[user] else 'PENDING'}\n"
        user_alert_section += "```"
        return user_alert_section

    users = []
    async for user in reaction.users():
        users.append(user)
    msgs = await _job(users)
    alerted_users = {}
    for user in users:
        alerted_users[user] = False

    update_msg += "**Done**\n"
    await message.edit(content=update_msg + get_user_alert_section(alerted_users))

    for user in users:
        await user.send(
            "Reminder: You are signed up for the event tonight.\n" + msgs[user]
        )
        alerted_users[user] = True
        await message.edit(content=update_msg + get_user_alert_section(alerted_users))

    await message.edit(
        content=update_msg + get_user_alert_section(alerted_users) + "\n**All done!**"
    )


@bot.event
async def on_ready():
    global LAST_POLL_MESSAGE
    channel = discord.utils.get(bot.get_all_channels(), id=ATTENDANCE_CHANNEL_ID)
    last_poll_message = None
    async for message in channel.history(limit=10):
        if message.author.id == int(PROBOT_ID):
            last_poll_message = message
            break
    logging.info(f"Auto-loading latest att poll: {last_poll_message.jump_url}")
    await update_att_sheet(last_poll_message)


bot.run(BOT_TOKEN)
