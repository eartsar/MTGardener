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
parser = argparse.ArgumentParser(description='Run MT Gardener')
parser.add_argument('--config', type=str, required=True, help='The path to the configuration yaml file.')
args = parser.parse_args()


# Load the configuration file
config = {}
with open(args.config, 'r') as f:
    config = yaml.safe_load(f)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(config['logging_path'] if 'logging_path' in config else 'bot.log'),
        logging.StreamHandler()
    ]
)

logging.info("Loading configuration...")
BOT_TOKEN = config['bot_token']
MT_SERVER_ID = config['server_id']
FEEDBACK_CHANNEL_ID = config['feedback_channel_id']
ATTENDANCE_CHANNEL_ID = config['attendance_channel_id']

intents = discord.Intents.default()
intents.messages = True
intents.message_content = True
intents.members = True
intents.reactions = True

bot = commands.Bot(command_prefix='!', case_insensitive=True, intents=intents)
bot.description = """MT Gardener is Mother Tree's little personal assistant bot.

It does little things to make life a little easier (hopefully) on the folks who wish to use it.
To use it, send the bot a DM with a command, like `!changelog` or `!help`.

Reach out to Barumaru with any feedback!"""

SUGGESTION_TEMPLATE = '''
**I've got a new suggestion to pass on!**

>>> {}
'''
SUGGESTION_LENGTH_MINIMUM = 20

logging.info("Loading Google Sheets integration...")
GOOGLE_CREDS_JSON = config['google_service_account_creds']
GOOGLE_SHEETS_URL = config['google_sheets_url']
JOB_SHEET_URL = config['job_sheet_url']

def get_creds():
    creds = Credentials.from_service_account_file(GOOGLE_CREDS_JSON)
    scoped = creds.with_scopes([
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ])
    return scoped

agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)

PROBOT_ID = int(config['probot_id'])

LAST_POLL_MESSAGE = None
LAST_POLL_CHANGED = True



# Custom check functions that can disallow commands from being run
async def check_channel_is_dm(ctx):
    return isinstance(ctx.channel, discord.channel.DMChannel)


async def check_user_is_council_or_dev(ctx):
    guild = bot.get_guild(MT_SERVER_ID)
    user = ctx.message.author
    member = discord.utils.find(lambda m: m.id == user.id, guild.members)
    role = discord.utils.find(lambda r: r.name in ('Elder Tree Council', 'MT Gardener Dev'), member.roles)
    return bool(role)


# Error handler
@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.errors.CheckFailure):
        pass


@bot.command()
@commands.check(check_channel_is_dm)
async def suggest(ctx):
    if len(ctx.message.content.split(' ')) < 2:
        return await ctx.send("Usage example: `!suggest I think that Barumaru should get all the loot from now on!`")
    elif len(ctx.message.content) < SUGGESTION_LENGTH_MINIMUM:
        return await ctx.send("Please elaborate a little bit more with your suggestion.")
    channel = discord.utils.get(bot.get_all_channels(), id=FEEDBACK_CHANNEL_ID)
    content = ctx.message.content[len('!suggest '):]
    suggestion_message = await channel.send(SUGGESTION_TEMPLATE.format(content))
    thread = await suggestion_message.create_thread(name="Suggestion Feedback")
    await thread.send(f"*Feel free to leave feedback and discuss this suggestion here. Please be civil!*")
    await ctx.send(f"I've passed along your suggestion. You can read the discussion here: {suggestion_message.jump_url}")


async def update_att_sheet(last_poll_message):
    print("Updating poll responses...")
    LAST_POLL_CHANGED = False
    reaction_map = {}
    for reaction in last_poll_message.reactions:
        reaction_map[reaction.emoji.name] = []
        async for user in reaction.users():
            if user.id == PROBOT_ID:
                continue
            reaction_map[reaction.emoji.name].append(f'{user.name}#{user.discriminator}')

    agc = await agcm.authorize()
    ss = await agc.open_by_url(GOOGLE_SHEETS_URL)
    ws = await ss.worksheet("Linkshell Roster")

    # Generate batch-update commands for each user so we don't get rate-limited
    # Fields that are already the right value no-op and are not added to batch update
    def add_to_batch(batch_updates, user_id, user_id_col_values, current_att_col_values, val):
        try:
            index = user_id_col_values.index(user_id)
            current = current_att_col_values[index]
            if current == val:
                return

            print(f'Updating att poll response for {user_id}')
            batch_updates.append({'range': f'W{index + 1}', 'values': [[val]]})
        except Exception as e:
            print(e)

    user_id_col_values = await ws.col_values(2)

    # determine the window of actual user names in the column, we need it to figure out non-responses
    start_index = user_id_col_values.index('Discord Tag') + 1
    end_index = user_id_col_values.index('', 6) - 1
    
    full_roster = set(user_id_col_values[start_index:end_index + 1])

    current_att_col_values = await ws.col_values(23)
    current_att_col_values.extend(['' for _ in range(len(user_id_col_values) - len(current_att_col_values))])
    

    update_map = {}
    for user_id in (reaction_map['attcheck'] if 'attcheck' in reaction_map else []) + \
            (reaction_map['attcheck2'] if 'attcheck2' in reaction_map else []):
        update_map[user_id] = 'o'
    
    for user_id in (reaction_map['attearly'] if 'attearly' in reaction_map else []) + \
            (reaction_map['attlate'] if 'attlate' in reaction_map else []) + \
            (reaction_map['attmaybe'] if 'attmaybe' in reaction_map else []):
        update_map[user_id] = '/'
    
    for user_id in reaction_map['attdecline'] if 'attdecline' in reaction_map else []:
        update_map[user_id] = 'x'

    for user_id in (full_roster - set(update_map.keys())):
        update_map[user_id] = ''
    
    batch_updates = []
    for key in update_map:
        add_to_batch(batch_updates, key, user_id_col_values, current_att_col_values, update_map[key])        

    await ws.batch_update(batch_updates)


@bot.command()
@commands.check(check_channel_is_dm)
async def job(ctx):
    user = ctx.message.author
    account_id = f'{user.name}#{user.discriminator}'

    agc = await agcm.authorize()
    ss = await agc.open_by_url(GOOGLE_SHEETS_URL)
    ws = await ss.worksheet("Linkshell Roster")
    col_values = await ws.col_values(2)
    row_indexes = [i for i, x in enumerate(col_values) if x == account_id][:2]
    if not row_indexes:
        return

    character_name_cell = await ws.acell(f'A{row_indexes[0] + 1}')
    character_name = character_name_cell.value

    alt_name = None
    if len(row_indexes) > 1:
        alt_name_cell = await ws.acell(f'A{row_indexes[1] + 1}')
        alt_name = alt_name_cell.value

    agc = await agcm.authorize()
    ss = await agc.open_by_url(JOB_SHEET_URL)
    ws = await ss.worksheet("Party Setup")
    col_values = await ws.col_values(2)

    try:
        row = col_values.index(character_name) + 1
        job_cell = await ws.acell(f'C{row}')
        job = job_cell.value

        alt_assigned = False
        if alt_name:
            try:
                alt_row = col_values.index(alt_name) + 1
                alt_job_cell = await ws.acell(f'C{alt_row}')
                alt_job = alt_job_cell.value
                alt_assigned = True
            except:
                pass

        msg_main = f'[{character_name}: **{job}**]'
        msg_sub = ''
        if alt_assigned:
            msg_sub = f'[{alt_name}: **{alt_job}**]'

        await ctx.send(f"{ctx.message.author.mention} - {msg_main} {msg_sub}")
    except:
        await ctx.send(f"{ctx.message.author.mention} - You're not on the job sheet.")


@bot.command()
async def ping(ctx):
    return await ctx.send("Pong!")


@bot.command()
@commands.check(check_channel_is_dm)
async def changelog(ctx):
    num_commits = 3
    version_content = subprocess.check_output(['git', 'log', '--use-mailmap', f'-n{num_commits}'])
    await ctx.send("Most recent changes:\n```" + str(version_content, 'utf-8') + "```")


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


@bot.event
async def on_ready():
    global LAST_POLL_MESSAGE

    channel = discord.utils.get(bot.get_all_channels(), id=ATTENDANCE_CHANNEL_ID)
    last_poll_message = None
    async for message in channel.history(limit=10):
        if message.author.id == int(PROBOT_ID):
            last_poll_message = message
            break
    print(f"Auto-loading latest att poll: {last_poll_message.jump_url}")
    await update_att_sheet(last_poll_message)



bot.run(BOT_TOKEN)
