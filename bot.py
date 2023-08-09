import discord
from discord.ext import commands, tasks

import traceback
import re
import subprocess
import argparse
import yaml
import logging
import arrow
from datetime import datetime
import parsedatetime
import time

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


def get_config_param_or_die(config, param):
    if param not in config:
        logging.error(
            f"Required parameter '{param}' is not present in the supplied configuration file."
        )
        import sys

        sys.exit(1)

    return config[param]


BOT_TOKEN = get_config_param_or_die(config, "bot_token")
MT_SERVER_ID = get_config_param_or_die(config, "server_id")
FEEDBACK_CHANNEL_ID = get_config_param_or_die(config, "feedback_channel_id")
FUTURE_OUTLOOK_ID = get_config_param_or_die(config, "future_outlook_id")
ALERT_CHANNEL_ID = get_config_param_or_die(config, "alert_channel_id")
ALERT_MESSAGE_ID = get_config_param_or_die(config, "alert_message_id")
EVENT_VOICE_CHANNEL_GROUP_ID = get_config_param_or_die(config, "event_channel_group_id")

ROSTER_SHEET_NAME = get_config_param_or_die(config, "roster_sheet_name")
PARTY_SHEET_NAME = get_config_param_or_die(config, "party_sheet_name")
DYNAMIS_WISHLIST_SHEET_NAME = get_config_param_or_die(
    config, "dynamis_wishlist_sheet_name"
)
PARTY_COMP_CHANNEL_ID = get_config_param_or_die(config, "party_comp_channel_id")

intents = discord.Intents.default()
intents.messages = True
intents.message_content = True
intents.members = True
intents.reactions = True


def get_creds():
    """Function to be called by the AsyncioGspreadClientManager to renew credentials when they expire"""
    creds = Credentials.from_service_account_file(GOOGLE_CREDS_JSON)
    scoped = creds.with_scopes(
        [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
    )
    return scoped


class MTBot(commands.Bot):
    def __init__(self, *args, **kwargs):
        super(MTBot, self).__init__(*args, **kwargs)
        self.agcm = None
        self.registered_dynamis_zone = None
        self.att_tracker = None
        self.att_tracking_start = None
        self.att_tracking_message = None

    async def setup_hook(self):
        """Orchestrate other async code to be on the same loop at startup"""
        self.agcm = gspread_asyncio.AsyncioGspreadClientManager(
            get_creds, loop=self.loop
        )
        sync_wishlists.start()


bot = MTBot(command_prefix="!", case_insensitive=True, intents=intents)
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
COUNCIL_SHEETS_URL = config["council_sheets_url"]

PROBOT_ID = int(config["probot_id"])


def shared_max_concurrency():
    """Modified implementation of MaxConcurrency so that it's a shared lock."""
    con_instance = commands.MaxConcurrency(
        1, per=commands.BucketType.default, wait=True
    )

    def decorator(func):
        if isinstance(func, commands.Command):
            func._max_concurrency = con_instance
        else:
            func.__commands_max_concurrency__ = con_instance
        return func

    return decorator


# Create the decorator - any command decorated with this will execute sequentially
sheets_access = shared_max_concurrency()


# Custom check functions that can disallow commands from being run
async def check_channel_is_dm(ctx):
    return isinstance(ctx.channel, discord.channel.DMChannel)


async def check_user_is_council_or_dev(ctx):
    guild = bot.get_guild(MT_SERVER_ID)
    user = ctx.message.author
    member = discord.utils.find(lambda m: m.id == user.id, guild.members)
    role = discord.utils.find(
        lambda r: r.name
        in ("Elder Tree Treants (Council)", "MT Gardener Dev", "Council Help"),
        member.roles,
    )
    return bool(role)


async def check_user_can_have_nice_things(ctx):
    guild = bot.get_guild(MT_SERVER_ID)
    user = ctx.message.author
    member = discord.utils.find(lambda m: m.id == user.id, guild.members)
    role = discord.utils.find(
        lambda r: r.name in ("Cannot Have Nice Things"),
        member.roles,
    )
    return not bool(role)


# Error handler
@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.errors.CheckFailure):
        pass


@bot.event
async def on_voice_state_update(member, before, after):
    event_channels = bot.get_channel(EVENT_VOICE_CHANNEL_GROUP_ID).voice_channels

    def entered_event_channel(before, after):
        return before.channel not in event_channels and after.channel in event_channels

    def exited_event_channel(before, after):
        return before.channel in event_channels and after.channel not in event_channels

    if bot.att_tracker:
        if str(member) not in bot.att_tracker:
            bot.att_tracker[str(member)] = []

        if entered_event_channel(before, after):
            bot.att_tracker[str(member)].append(arrow.now())
        elif exited_event_channel(before, after):
            bot.att_tracker[str(member)].append(arrow.now())


@bot.command()
@commands.check(check_channel_is_dm)
@commands.check(check_user_can_have_nice_things)
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


async def verified_reactions_to_last_outlook():
    # Get the last posted outlook message
    channel = bot.get_channel(FUTURE_OUTLOOK_ID)
    last_outlook_message = None
    async for message in channel.history(limit=10):
        if message.author.id == int(PROBOT_ID):
            last_outlook_message = message
            break

    if not last_outlook_message:
        return []

    reaction_map = {}
    for reaction in last_outlook_message.reactions:
        if type(reaction.emoji) == str:
            continue
        reaction_map[reaction.emoji.name] = []
        async for user in reaction.users():
            if user.id == PROBOT_ID:
                continue
            reaction_map[reaction.emoji.name].append(user)

    can_go = []
    can_go.extend(reaction_map["verifygreen"] if "verifygreen" in reaction_map else [])
    can_go.extend(reaction_map["verifypink"] if "verifypink" in reaction_map else [])
    can_go.extend(reaction_map["verifyteal"] if "verifyteal" in reaction_map else [])
    return can_go


@bot.command()
@commands.check(check_channel_is_dm)
@sheets_access
async def job(ctx):
    msgs = await _job([ctx.message.author])
    if ctx.author in msgs:
        await ctx.author.send(msgs[ctx.author])
    else:
        await ctx.author.send(
            "I couldn't find you in the LS roster. Check with council that you're properly added."
        )


async def get_roster_for_users(users):
    # Fetch the roster range that contains username, main, and alt name
    agc = await bot.agcm.authorize()
    council_ss = await agc.open_by_url(COUNCIL_SHEETS_URL)
    roster_ws = await council_ss.worksheet("Wishlist Submissions")

    roster_range = await roster_ws.get_values("A:D")
    roster = {}
    for i in range(1, len(roster_range)):
        row = roster_range[i]
        roster[row[3]] = {
            "main": row[0],
            "alt": row[1] if row[1] else None,
            "id": row[3],
            "row_index": i + 1,
        }

    # Log which users we wanted from the roster, but aren't present.
    usernames_not_in_roster = [str(user) for user in users if str(user) not in roster]
    if usernames_not_in_roster:
        logging.warning(f"Roster info not present for users: {usernames_not_in_roster}")

    # Convert the output to be indexable by user object, rather than the ID of the user
    users_in_roster = [user for user in users if str(user) in roster]
    return {user: roster[str(user)] for user in users_in_roster}


async def _job(users):
    msgs = {}
    roster = {}
    try:
        roster = await get_roster_for_users(users)
    except Exception as e:
        logging.error(f"Something went wrong when trying to get the roster. {e}")
        return msgs

    agc = await bot.agcm.authorize()
    party_ss = await agc.open_by_url(JOB_SHEETS_URL)
    party_ws = await party_ss.worksheet(PARTY_SHEET_NAME)

    logging.info("Pulling job comp sheet assigned chracters...")

    def get_job_assignment(name, job_map):
        return job_map[name] if (name and name in job_map) else None

    try:
        job_map = {_[0]: _[1] for _ in await party_ws.get_values("B:C")}

        for user in users:
            if user not in roster:
                logging.warning(
                    f"{user} was not found in the roster. Double-check that they are on it."
                )
                continue
            main_name = roster[user]["main"]
            alt_name = roster[user]["alt"]

            logging.info(roster[user])

            main_job = get_job_assignment(roster[user]["main"], job_map)
            alt_job = get_job_assignment(roster[user]["alt"], job_map)

            if main_job or alt_job:
                msg = ""
                if main_job:
                    msg += (
                        f"[{main_name}: **{main_job if main_job else 'Unspecified'}**] "
                    )
                if alt_job:
                    msg += f"[{alt_name}: **{alt_job if alt_job else 'Unspecified'}**]"

                msgs[user] = f"{user.mention} - {msg}"
            else:
                msgs[user] = f"{user.mention} - You're not on the job sheet."

        logging.info("Done fetching jobs for users.")
        logging.debug(msgs)
        return msgs
    except Exception as e:
        logging.error(traceback.format_exc())


@bot.command()
async def ping(ctx):
    return await ctx.send("Pong!")


@bot.command()
async def changelog(ctx):
    num_commits = 3
    version_content = subprocess.check_output(
        ["git", "log", "--use-mailmap", f"-n{num_commits}"]
    )
    await ctx.send("Most recent changes:\n```" + str(version_content, "utf-8") + "```")


@bot.command()
@commands.check(check_channel_is_dm)
@commands.check(check_user_is_council_or_dev)
@sheets_access
async def publishjobs(ctx):
    msg = await construct_joblist_message()
    party_channel = discord.utils.get(bot.get_all_channels(), id=PARTY_COMP_CHANNEL_ID)
    await party_channel.send(msg)


async def construct_joblist_message():
    agc = await bot.agcm.authorize()
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
@sheets_access
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
                user_alert_section += f"{str(user)} - {alerted_status[user]}\n"
            user_alert_section += "```"
            return user_alert_section

        users = []
        async for user in reaction.users():
            users.append(user)

        logging.info("Cross-referencing latest attendance poll...")
        update_msg += "**Done**\n*Filtering out folks on hiatus who didn't sign up...* "
        await message.edit(content=update_msg)
        can_go = set(await verified_reactions_to_last_outlook())

        def is_on_hiatus(user):
            for role in user.roles:
                if "hiatus" in role.name.lower():
                    return True
            return False

        on_hiatus = set([_ for _ in users if is_on_hiatus(user)])
        users = [_ for _ in users if _ not in (on_hiatus - can_go)]

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
                    f"{str(user)} wasn't found in the roster. Double-check that they are added."
                )
                alerted_status[user] = "FAILED"
            elif test:
                test_content += f"\n{msgs[user]}"
                alerted_status[user] = "DONE"
                await test_message.edit(content=test_content)
            else:
                try:
                    await user.send(
                        "Reminder: You are signed up for the event tonight.\n"
                        + msgs[user]
                    )
                    alerted_status[user] = "DONE"
                except:
                    alerted_status[user] = "FAILED"

            await message.edit(
                content=update_msg + get_user_alert_section(alerted_status)
            )

        await message.edit(
            content=update_msg
            + get_user_alert_section(alerted_status)
            + "\n**All done!**"
        )

        comp_msg = await construct_joblist_message()
        party_channel = discord.utils.get(
            bot.get_all_channels(), id=PARTY_COMP_CHANNEL_ID
        )
        if test:
            await ctx.send("The following job comp would be posted:\n" + comp_msg)
        else:
            await party_channel.send(comp_msg)

    except Exception as e:
        logging.error(e)


@bot.command()
@commands.check(check_user_is_council_or_dev)
@sheets_access
async def dyna(ctx):
    try:
        zone_anchors = {
            "bastok": 2,
            "jeuno": 6,
            "sandy": 10,
            "windy": 14,
            "beau": 18,
            "xarc": 22,
            "bubu": 27,
            "qufim": 36,
            "valkurm": 45,
            "tavnazia": 54,
        }
        invalid_zone_msg = f"Zone must be registered to one of the following zones. ```{', '.join(zone_anchors)}``` For example... ```!dyna zone jeuno```"

        tokens = [token.lower() for token in ctx.message.content.split(" ")]
        if len(tokens) not in (2, 3):
            return await ctx.send(
                "Usage example: `!dyna WHM` or `!dyna BLU acc` or `!dyna COR -1`"
            )

        if tokens[1].lower() == "zone" and len(tokens) >= 3:
            if tokens[2].lower() not in zone_anchors:
                return await ctx.send(invalid_zone_msg)
            else:
                bot.registered_dynamis_zone = tokens[2].lower()
                return await ctx.send(
                    f"Current dynamis zone set to: `{bot.registered_dynamis_zone}`"
                )
        elif not bot.registered_dynamis_zone:
            return await ctx.send(invalid_zone_msg)

        job = tokens[1]
        choice_type = "af" if len(tokens) != 3 else tokens[2]
        valid_jobs = (
            "war",
            "mnk",
            "whm",
            "blm",
            "rdm",
            "thf",
            "pld",
            "drk",
            "bst",
            "brd",
            "rng",
            "sam",
            "nin",
            "drg",
            "smn",
            "blu",
            "cor",
            "pup",
        )
        valid_choice_types = ("af", "-1", "acc")

        if job not in valid_jobs:
            return await ctx.send("That is not a valid job.")
        elif choice_type not in valid_choice_types:
            return await ctx.send(
                "That is not a valid drop choice. Must be either af (default), -1, or acc."
            )

        agc = await bot.agcm.authorize()
        ss = await agc.open_by_url(COUNCIL_SHEETS_URL)
        ws = await ss.worksheet(DYNAMIS_WISHLIST_SHEET_NAME)

        msg = f"Loot List for **{job.upper()} [{choice_type.upper()}]**\n"
        newline = "\n"
        tics = "```"

        choice_one_index = zone_anchors[bot.registered_dynamis_zone] + 1
        choice_two_index = zone_anchors[bot.registered_dynamis_zone] + 2
        choice_other_index = zone_anchors[bot.registered_dynamis_zone] + 3
        choice_minus_one_index = zone_anchors[bot.registered_dynamis_zone] + 4
        choice_acc_index = zone_anchors[bot.registered_dynamis_zone] + 6
        choice_acc_other_index = zone_anchors[bot.registered_dynamis_zone] + 8

        character_name_values = await ws.col_values(1)
        if choice_type == "af":
            choice_one = await ws.col_values(choice_one_index)
            choice_two = await ws.col_values(choice_two_index)
            choice_other = await ws.col_values(choice_other_index)

            logging.info(character_name_values)
            who_ones = [
                character_name_values[i]
                for i, v in enumerate(choice_one)
                if job.lower() in v.lower()
            ]
            who_twos = [
                character_name_values[i]
                for i, v in enumerate(choice_two)
                if job.lower() in v.lower()
            ]
            who_others = [
                character_name_values[i]
                for i, v in enumerate(choice_other)
                if job.lower() in v.lower()
            ]

            if who_ones or who_twos or who_others:
                msg += f'**First Choice**{(tics + newline + newline.join(who_ones) + tics) if who_ones else "``` ```"}'
                msg += f'**Second Choice**{(tics + newline + newline.join(who_twos) + tics) if who_twos else "``` ```"}'
                msg += f'**Other**{(tics + newline + newline.join(who_others) + tics) if who_others else "``` ```"}'
            else:
                msg += "```\nFREE LOT```"

            await ctx.send(msg)
        else:
            if bot.registered_dynamis_zone not in (
                "bubu",
                "qufim",
                "valkurm",
                "tavnazia",
            ):
                return await ctx.send(
                    "Current dynamis zone must be a dreamlands zone to do that."
                )
            choice_minus_one = await ws.col_values(choice_minus_one_index)
            choice_acc = await ws.col_values(choice_acc_index)
            choice_other = await ws.col_values(choice_acc_other_index)

            who_ones = None
            if choice_type == "-1":
                who_ones = [
                    character_name_values[i]
                    for i, v in enumerate(choice_minus_one)
                    if job.lower() in v.lower()
                ]
            else:
                who_ones = [
                    character_name_values[i]
                    for i, v in enumerate(choice_acc)
                    if job.lower() in v.lower()
                ]

            mapping = str.maketrans("", "", ' ,."')
            who_others = [
                character_name_values[i]
                for i, v in enumerate(choice_other)
                if f"{job}{choice_type}" in v.lower().translate(mapping)
            ]

            print([v.lower().translate(mapping) for i, v in enumerate(choice_other)])
            if who_ones or who_others:
                msg += f'**First Choice**{(tics + newline + newline.join(who_ones) + tics) if who_ones else "``` ```"}'
                msg += f'**Other**{(tics + newline + newline.join(who_others) + tics) if who_others else "``` ```"}'
            else:
                msg += "```\nFREE LOT```"

            await ctx.send(msg)
    except Exception as e:
        logging.error(traceback.format_exc())


@bot.command()
async def wishlist(ctx):
    return await sync(ctx, link="link")


@bot.command()
@sheets_access
async def sync(ctx, link=None):
    logging.info("Wishlist request initiated.")

    logging.info("Authorizing Google Sheets...")
    agc = await bot.agcm.authorize()
    council_ss = await agc.open_by_url(COUNCIL_SHEETS_URL)

    async def fetch_wishlist_url(author):
        author_id = str(author).lower()
        logging.info(f"Fetching wishlist URL for {author_id}")

        wishlist_lookups = await council_ss.worksheet("Wishlist Submissions")

        discord_ids = [_.lower() for _ in await wishlist_lookups.col_values(4)]
        discord_id_index = discord_ids.index(author_id) + 1
        wishlist_url = (await wishlist_lookups.get_values(f"E{discord_id_index}"))[0][0]
        return wishlist_url

    wishlist_url = None
    try:
        if not link:
            wishlist_url = await fetch_wishlist_url(ctx.author)
        elif link == "link":
            # Special case: "!wishlist link" --> Get a link to the requester's wishlist.
            wishlist_url = await fetch_wishlist_url(ctx.author)
            await ctx.message.add_reaction("👀")
            return await ctx.author.send(f"Your wishlist link: {wishlist_url}")
        else:
            wishlist_url = link
    except Exception as e:
        logging.error(e)
        return await ctx.send(
            "ERROR: Check with council to make sure your wishlist is registered."
        )

    update_msg = "Syncing wishlist with council's sheet... "
    message = await ctx.send(update_msg)

    logging.info("Fetching wishlist sheet reference...")
    wishlist_ss = None
    try:
        wishlist_ss = await agc.open_by_url(wishlist_url)
    except Exception as e:
        logging.error(e)
        return await ctx.send("ERROR: Wishlist URL is not valid.")

    await _sync_apply(wishlist_ss, council_ss)
    return await message.edit(content=update_msg + "**Done!**")


@bot.command()
@commands.check(check_user_is_council_or_dev)
async def kys(ctx):
    import os, sys

    await ctx.send("💀 byebye...")
    try:
        os.execv(sys.executable, ["python"] + sys.argv)
    except Exception as e:
        logging.error("Exception " + str(e))


@sheets_access
@tasks.loop(minutes=15.0)
async def sync_wishlists():
    logging.info("Syncing all wishlists...")
    logging.info("Force-pulling an access token for google drive metadata lookups...")

    import google.auth.transport.requests
    import aiohttp

    async def fetch(url, session):
        async with session.get(url) as resp:
            return await resp.json()

    try:
        agc = await bot.agcm.authorize()
        request = google.auth.transport.requests.Request()
        agc.gc.auth.refresh(request)
        drive_access_token = "Bearer " + agc.gc.auth.token

        council_ss = await agc.open_by_url(COUNCIL_SHEETS_URL)
        logging.info("Pulling links and timestamps...")
        wishlist_ws = await council_ss.worksheet("Wishlist Submissions")
        wishlist_rows = await wishlist_ws.get_values("A:F")
        ss_id_to_timestamps = {}

        async with aiohttp.ClientSession(
            headers={"Authorization": drive_access_token}
        ) as session:
            drive_urls = []
            for i in range(1, len(wishlist_rows)):
                wishlist_url = wishlist_rows[i][4]
                if wishlist_rows[i][5] == "TRUE":
                    continue

                if not wishlist_url:
                    continue

                ss_id = None
                try:
                    ss_id = (
                        re.compile(
                            r".+docs.google.com\/spreadsheets\/d\/(.+?)\/?(?:\/.+)?$"
                        )
                        .match(wishlist_url)
                        .group(1)
                    )
                except Exception as e:
                    logging.error(f"Invalid URL: " + wishlist_url)
                    continue

                drive_urls.append(
                    f"https://www.googleapis.com/drive/v3/files/{ss_id}?supportsAllDrives=true&fields=name,modifiedTime,webViewLink,id"
                )
                ss_id_to_timestamps[ss_id] = wishlist_rows[i][2]

            logging.info("Executing parallel requests for wishlist metadata...")
            tasks = []
            for drive_url in drive_urls:
                task = asyncio.ensure_future(fetch(drive_url, session))
                tasks.append(task)

            responses = await asyncio.gather(*tasks)
            logging.info("Done. Checking to see which lists need updating...")

            usernames_no_update_needed = []
            for wishlist_metadata in responses:
                mod_str = wishlist_metadata["modifiedTime"]
                upd_str = ss_id_to_timestamps[wishlist_metadata["id"]]
                web_link = wishlist_metadata["webViewLink"]
                ss_name = wishlist_metadata["name"]
                if not upd_str:
                    logging.info(f"{ss_name} has never been updated. Updating...")
                    wishlist_ss = await agc.open_by_url(web_link)
                    await _sync_apply(wishlist_ss, council_ss)
                elif arrow.get(mod_str) > arrow.get(upd_str):
                    delta = arrow.now() - arrow.get(mod_str)
                    delta_str = (
                        f"{delta.days} days ago"
                        if delta.days
                        else f"{delta.seconds} seconds ago"
                    )
                    logging.info(f"{ss_name} is out of date ({delta_str}). Updating...")
                    wishlist_ss = await agc.open_by_url(web_link)
                    await _sync_apply(wishlist_ss, council_ss)
                else:
                    usernames_no_update_needed.append(ss_name)

            logging.info(
                f"{usernames_no_update_needed} - Up to date, no update needed."
            )
    except Exception as e:
        logging.error(
            f"An error occurred while syncing wishlists. {traceback.format_exc()}"
        )


async def _sync_apply(wishlist_ss, council_ss):
    from loot_mappings import (
        DYNAMIS_MAIN,
        DYNAMIS_ALT,
        SKY_MAIN,
        SKY_ALT,
        SEA_MAIN,
        SEA_ALT,
        LIMBUS_MAIN,
        LIMBUS_ALT,
    )

    logging.info(f"Applying wishlist sync for {wishlist_ss.title}...")
    council_dynamis_ws = await council_ss.worksheet("Dynamis Wishlists")
    council_sky_ws = await council_ss.worksheet("Sky Requests")
    council_sea_ws = await council_ss.worksheet("Sea Requests")
    council_limbus_ws = await council_ss.worksheet("Limbus Requests")

    logging.info("Looking up character names for wishlist sync...")
    charname_main = None
    charname_alt = None
    try:
        charname_main = (
            (await wishlist_ss.values_get("INSTRUCTIONS!D2"))["values"][0][0]
            .lower()
            .strip()
        )
        charname_alt = (
            (await wishlist_ss.values_get("INSTRUCTIONS!F2"))["values"][0][0]
            .lower()
            .strip()
        )
    except Exception as e:
        pass

    if not charname_main:
        logging.error("Character names are not filled out!")
        return

    logging.info(
        f"  Syncing wishlist items for {charname_main}{' and ' + charname_alt if charname_alt else ''}..."
    )

    async def push_wishlist_updates(charname, mapping, wishlist_ss, council_ws):
        dest = next(iter(mapping))
        dest = dest[: dest.index("!")]
        logging.info(f"{charname}: pulling from {dest}")
        charname_col_values = [_.lower() for _ in await council_ws.col_values(1)]

        row_index = None
        try:
            row_index = charname_col_values.index(charname) + 1
        except:
            logging.warning(f"Could not find {charname} in {council_ws}")
            return

        batch_gets = list(mapping.keys())
        items_to_push = await wishlist_ss.values_batch_get(ranges=batch_gets)

        logging.info(f"{charname}: pushing to council sheet {council_ws.title}")
        batch_updates = []
        batch_clears = []
        for item in items_to_push["valueRanges"]:
            destination = mapping[item["range"]] + str(row_index)
            if "values" in item:
                batch_updates.append({"range": destination, "values": item["values"]})
            else:
                batch_clears.append(destination)

        await council_ws.batch_clear(batch_clears)
        return await council_ws.batch_update(batch_updates)

    dynamis_megadict_main = {k: v for d in DYNAMIS_MAIN for k, v in d.items()}
    dynamis_megadict_alt = {k: v for d in DYNAMIS_ALT for k, v in d.items()}

    try:
        # Main
        await push_wishlist_updates(
            charname_main, dynamis_megadict_main, wishlist_ss, council_dynamis_ws
        )
        await push_wishlist_updates(
            charname_main, SKY_MAIN, wishlist_ss, council_sky_ws
        )
        await push_wishlist_updates(
            charname_main, SEA_MAIN, wishlist_ss, council_sea_ws
        )
        await push_wishlist_updates(
            charname_main, LIMBUS_MAIN, wishlist_ss, council_limbus_ws
        )

        # Alt
        if charname_alt:
            await push_wishlist_updates(
                charname_alt, dynamis_megadict_alt, wishlist_ss, council_dynamis_ws
            )
            await push_wishlist_updates(
                charname_alt, SKY_ALT, wishlist_ss, council_sky_ws
            )
            await push_wishlist_updates(
                charname_alt, SEA_ALT, wishlist_ss, council_sea_ws
            )
            await push_wishlist_updates(
                charname_alt, LIMBUS_ALT, wishlist_ss, council_limbus_ws
            )
    except Exception as e:
        logging.error(e)

    # TODO: clean this up
    wishlist_lookups = await council_ss.worksheet("Wishlist Submissions")
    charnames = [_.lower() for _ in (await wishlist_lookups.col_values(1))]
    update_index = charnames.index(charname_main.lower()) + 1
    await wishlist_lookups.update_acell(f"C{update_index}", str(arrow.utcnow()))
    logging.info("Done!")
    return


@bot.command()
@commands.check(check_user_can_have_nice_things)
async def reminder(ctx):
    regex = re.compile(r"!reminder(?:\s(to .+))?\s((?:in|on|at)[a-zA-Z0-9\s]+)(@.+)?")
    res = regex.search(ctx.message.content)
    if not res:
        return await ctx.message.reply(
            "Usage: `!reminder [to <reason>] in|on|at <date and/or time> [@person @role ...]`"
        )

    try:
        matches = res.groups()
        when = matches[1]

        cal = parsedatetime.Calendar()
        time_struct, _ = cal.parse(when)
        target_time = time.mktime(time_struct)

        async def remind_in(when, msg, mentions_section):
            delay = when - time.mktime(datetime.now().timetuple())
            await asyncio.sleep(delay)
            await msg.reply(
                f"⏰ {mentions_section}This is your reminder for the thing! ⏰"
            )

        all_mentions = ctx.message.mentions + ctx.message.role_mentions
        mentions_section = (
            ""
            if not all_mentions
            else f"{' '.join([_.mention for _ in all_mentions])} - "
        )
        # send a message response
        await ctx.message.reply(f"⏰ I will remind you at <t:{int(target_time)}> ⏰")

        # spin up the reminder
        await remind_in(target_time, ctx.message, mentions_section)
    except Exception as e:
        logging.error(traceback.format_exc())


@bot.command()
async def att(ctx, state, event_name=None):
    try:
        event_channels = bot.get_channel(EVENT_VOICE_CHANNEL_GROUP_ID).voice_channels
        if state == "start":
            bot.att_tracker = {}
            bot.att_tracking_start = arrow.now()
            bot.att_tracking_message = await ctx.message.reply(
                f"Starting attendance tracking{(' **for ' + event_name + '**') if event_name else ''}."
            )
            for event_channel in event_channels:
                for event_member in event_channel.members:
                    if str(event_member) not in bot.att_tracker:
                        bot.att_tracker[str(event_member)] = []
                    bot.att_tracker[str(event_member)].append(arrow.now())

        elif state == "stop":
            att_delta = arrow.now() - bot.att_tracking_start
            bot.att_tracking_start = None
            for event_channel in event_channels:
                for event_member in event_channel.members:
                    if str(event_member) not in bot.att_tracker:
                        bot.att_tracker[str(event_member)] = []
                    bot.att_tracker[str(event_member)].append(arrow.now())
            results = {}
            for user in bot.att_tracker:
                timestamps = bot.att_tracker[user]
                pairs = [
                    (timestamps[i], timestamps[i - 1])
                    for i in range(len(timestamps) - 1, 0, -2)
                ]
                total = 0
                for end, start in pairs:
                    total += (end - start).seconds

                # hacky work-around, if the total time is EXACTLY a half hour increment
                # add in one second to avoid banker's rounding
                if total % (60 * 30) == 0:
                    total += 1

                results[user] = (round(total / (60 * 60)), total / att_delta.seconds)

            discord_users_tracked = [
                _ for _ in bot.get_guild(MT_SERVER_ID).members if str(_) in results
            ]
            roster = await get_roster_for_users(discord_users_tracked)
            points_lookup = {
                roster[user]["main"]: results[str(user)][0]
                for user in discord_users_tracked
            }
            await bot.att_tracking_message.reply(
                f"Stopping attendance tracking. Points: ```{points_lookup}```"
            )
            bot.att_tracker = None
            bot.att_tracking_message = None
    except Exception as e:
        logging.error(traceback.format_exc())


@bot.listen()
async def on_ready():
    logging.info("Bot is ready!")


async def main():
    # start the client
    async with bot:
        await bot.start(BOT_TOKEN)


asyncio.run(main())
