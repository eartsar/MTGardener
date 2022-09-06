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

ROSTER_SHEET_NAME = config["roster_sheet_name"]
PARTY_SHEET_NAME = config["party_sheet_name"]
DYNAMIS_WISHLIST_SHEET_NAME = config["dynamis_wishlist_sheet_name"]
PARTY_COMP_CHANNEL_ID = config["party_comp_channel_id"]

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
JOB_SHEETS_URL = config["job_sheets_url"]

SHEETS_LOCK = asyncio.Lock()


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


async def att_poll_reactions(last_poll_message):
    reaction_map = {}
    for reaction in last_poll_message.reactions:
        if type(reaction.emoji) == str:
            continue
        reaction_map[reaction.emoji.name] = []
        async for user in reaction.users():
            if user.id == PROBOT_ID:
                continue
            reaction_map[reaction.emoji.name].append(
                f"{user.name}#{user.discriminator}"
            )

    return reaction_map


async def get_last_poll_message():
    channel = discord.utils.get(bot.get_all_channels(), id=ATTENDANCE_CHANNEL_ID)
    async for message in channel.history(limit=10):
        if message.author.id == int(PROBOT_ID):
            return message


async def update_att_sheet(last_poll_message):
    logging.info("Updating poll responses...")

    reaction_map = await att_poll_reactions(last_poll_message)
    agc = await agcm.authorize()
    ss = await agc.open_by_url(GOOGLE_SHEETS_URL)
    ws = await ss.worksheet(ROSTER_SHEET_NAME)

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
    logging.info("Att poll updates done.")


@bot.command()
@commands.check(check_channel_is_dm)
async def job(ctx):
    msgs = await _job([ctx.message.author])
    if ctx.author in msgs:
        await ctx.author.send(msgs[ctx.author])
    else:
        await ctx.author.send(
            "I couldn't find you in the LS roster. Check with council that you're properly added."
        )


async def get_char_names_for_users(users):
    agc = await agcm.authorize()
    roster_ss = await agc.open_by_url(GOOGLE_SHEETS_URL)
    roster_ws = await roster_ss.worksheet(ROSTER_SHEET_NAME)

    logging.info("Fetching character names...")
    logging.info("  Pulling discord tags col from sheet...")
    col_values = await roster_ws.col_values(2)

    logging.info("  Mapping discord user to rows in roster sheet...")
    row_indexes = {}
    for user in users:
        account_id = f"{user.name}#{user.discriminator}"
        row_indexes[user.id] = [i for i, x in enumerate(col_values) if x == account_id][
            :2
        ]

    name_map = {}
    for user in users:
        logging.info(f"  Populating name map for {user}")
        if not row_indexes[user.id]:
            logging.warning(f"Cannot locate {user} on the roster sheet")
            continue

        user_row_indexes = row_indexes[user.id]
        character_name_cell = await roster_ws.acell(f"A{user_row_indexes[0] + 1}")
        character_name = character_name_cell.value

        alt_name = None
        if len(user_row_indexes) > 1:
            alt_name_cell = await roster_ws.acell(f"A{user_row_indexes[1] + 1}")
            alt_name = alt_name_cell.value

        name_map[user] = {"main": character_name, "alt": alt_name}

    logging.info("  Done fetching character names!")
    return name_map


async def _job(users):
    msgs = {}
    character_names = {}
    try:
        character_names = await get_char_names_for_users(users)
    except Exception as e:
        logging.error(f"Something went wrong when trying to get the roster. {e}")
        return msgs

    agc = await agcm.authorize()
    party_ss = await agc.open_by_url(JOB_SHEETS_URL)
    party_ws = await party_ss.worksheet(PARTY_SHEET_NAME)

    logging.info("Pulling job comp sheet assigned chracters...")
    col_values = await party_ws.col_values(2)
    logging.info(col_values)

    for user in users:
        logging.info(f"Checking character assignments for {user}...")
        if user not in character_names:
            logging.info(
                f"{user} was not found in the roster. Double-check that they are on it."
            )
            continue
        character_name = character_names[user]["main"]
        alt_name = character_names[user]["alt"]

        on_sheet = False
        alt_assigned = False
        main_assigned = False
        try:
            if character_name:
                row = col_values.index(character_name) + 1
                job_cell = await party_ws.acell(f"C{row}")
                job = job_cell.value
                main_assigned = True
                on_sheet = True
        except Exception as e:
            logging.error(e)
            pass

        try:
            if alt_name:
                alt_row = col_values.index(alt_name) + 1
                alt_job_cell = await party_ws.acell(f"C{alt_row}")
                alt_job = alt_job_cell.value
                alt_assigned = True
                on_sheet = True
        except Exception as e:
            logging.error(e)
            pass

        if on_sheet:
            msg_main = ""
            msg_sub = ""
            if main_assigned:
                msg_main = f"[{character_name}: **{job if job else 'Unspecified'}**] "
            if alt_assigned:
                msg_sub = f"[{alt_name}: **{alt_job if alt_job else 'Unspecified'}**]"

            msgs[user] = f"{user.mention} - {msg_main}{msg_sub}"
        else:
            msgs[user] = f"{user.mention} - You're not on the job sheet."

    logging.info("Done fetching jobs for users.")
    logging.info(msgs)
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
    last_poll_message = await get_last_poll_message()
    await update_att_sheet(last_poll_message)


@bot.command()
@commands.check(check_channel_is_dm)
@commands.check(check_user_is_council_or_dev)
async def publishjobs(ctx):
    msg = await construct_joblist_message()
    party_channel = discord.utils.get(bot.get_all_channels(), id=PARTY_COMP_CHANNEL_ID)
    await party_channel.send(msg)


async def construct_joblist_message():
    agc = await agcm.authorize()
    party_ss = await agc.open_by_url(JOB_SHEETS_URL)
    party_ws = await party_ss.worksheet(PARTY_SHEET_NAME)
    data = await party_ws.get_all_values()

    msg = "```"
    for row in range(1, 43):
        if (row - 1) % 7 == 0:
            msg += f"\n{data[row][0]}\n"
        else:

            name = data[row][1]
            job = data[row][2]
            note = data[row][0]
            line = f"  {name} {('(' + job + ')') if job else ''}{('      [' + note + ']') if note else ''}\n"
            if line.strip():
                msg += line

    msg += "```"
    return msg


@bot.command()
@commands.check(check_channel_is_dm)
@commands.check(check_user_is_council_or_dev)
async def alertjobs(ctx):
    test = "test" in ctx.message.content
    try:
        update_msg = "*Grabbing users who have subscribed to alerts...* "
        message = await ctx.send(update_msg)

        alert_channel = discord.utils.get(bot.get_all_channels(), id=ALERT_CHANNEL_ID)
        sub_message = await alert_channel.fetch_message(ALERT_MESSAGE_ID)
        reaction = discord.utils.get(sub_message.reactions, emoji="📣")

        def get_user_alert_section(alerted_status):
            if not alerted_status:
                return "```\nAin't nobody here but us chickens!```"

            user_alert_section = "```"
            for user in alerted_status:
                user_alert_section += (
                    f"{user.name}#{user.discriminator} - {alerted_status[user]}\n"
                )
            user_alert_section += "```"
            return user_alert_section

        users = []
        async for user in reaction.users():
            users.append(user)

        logging.info("Cross-referencing latest attendance poll...")
        update_msg += (
            "**Done**\n*Checking attendance poll to omit those who can't go...* "
        )
        await message.edit(content=update_msg)
        last_poll_message = await get_last_poll_message()
        reaction_map = await att_poll_reactions(last_poll_message)
        decline_tags = set(
            reaction_map["attdecline"] if "attdecline" in reaction_map else []
        )

        logging.info("Omitting users who declined event: " + ", ".join(decline_tags))
        users = [_ for _ in users if f"{_.name}#{_.discriminator}" not in decline_tags]

        update_msg += "**Done**\n*Fetching users' jobs...* "
        await message.edit(content=update_msg)
        msgs = await _job(users)

        alerted_status = {}
        for user in users:
            alerted_status[user] = "PENDING"

        update_msg += "**Done** \n\n**Dispatching alerts to the following users!** "
        await message.edit(content=update_msg + get_user_alert_section(alerted_status))

        test_message = None
        test_content = "Simulated alerts...\n"
        if test:
            test_message = await ctx.send(test_content)

        for user in users:
            if user not in msgs:
                await ctx.send(
                    f"{user.name}#{user.discriminator} wasn't found in the roster. Double-check that they are added."
                )
                alerted_status[user] = "FAILED"
            elif test:
                test_content += f"\n{msgs[user]}"
                alerted_status[user] = "DONE"
                await test_message.edit(content=test_content)
            else:
                await user.send(
                    "Reminder: You are signed up for the event tonight.\n" + msgs[user]
                )
                alerted_status[user] = "DONE"

            await message.edit(
                content=update_msg + get_user_alert_section(alerted_status)
            )

        await message.edit(
            content=update_msg
            + get_user_alert_section(alerted_status)
            + "\n**All done!**"
        )

        comp_msg = await construct_joblist_message()
        party_channel = discord.utils.get(bot.get_all_channels(), id=PARTY_COMP_CHANNEL_ID)
        if test:
            await ctx.send("The following job comp would be posted:\n" + comp_msg)
        else:
            await party_channel.send(comp_msg)

    except Exception as e:
        logging.error(e)


@bot.command()
@commands.check(check_user_is_council_or_dev)
async def dyna(ctx):
    if len(ctx.message.content.split(" ")) != 2:
        return await ctx.send("Usage example: `!dyna WHM`")

    job = ctx.message.content.split(" ")[1]
    valid_jobs = ('war', 'mnk', 'whm', 'blm', 'rdm', 'thf', 'pld', 'drk', 'bst',
        'brd', 'rng', 'sam', 'nin', 'drg', 'smn', 'blu', 'cor', 'pup')
    if job not in valid_jobs:
        return await ctx.send("That is not a valid job.")

    agc = await agcm.authorize()
    ss = await agc.open_by_url(GOOGLE_SHEETS_URL)
    ws = await ss.worksheet(DYNAMIS_WISHLIST_SHEET_NAME)

    character_name_values = await ws.col_values(1)
    choice_one = await ws.col_values(2)
    choice_two = await ws.col_values(3)
    choice_other = await ws.col_values(4)


    who_ones = [character_name_values[i] for i,v in enumerate(choice_one) if job.lower() in v.lower()]
    who_twos = [character_name_values[i] for i,v in enumerate(choice_two) if job.lower() in v.lower()]
    who_others = [character_name_values[i] for i,v in enumerate(choice_other) if job.lower() in v.lower()]

    newline = "\n"
    tics = "```"
    msg = f'Loot List for **{job.upper()}**\n'
    if who_ones or who_twos or who_others:
        msg += f'**First Choice**{(tics + newline + newline.join(who_ones) + tics) if who_ones else "``` ```"}'
        msg += f'**Second Choice**{(tics + newline + newline.join(who_twos) + tics) if who_twos else "``` ```"}'
        msg += f'**Other**{(tics + newline + newline.join(who_others) + tics) if who_others else "``` ```"}'
    else:
        msg += '```\nFREE LOT```'
    
    await ctx.send(msg)



@bot.event
async def on_ready():
    logging.info("Bot is ready!")


bot.run(BOT_TOKEN)
