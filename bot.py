import discord
from discord.ext import commands

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
bot = commands.Bot(command_prefix='!', intents=intents)

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

PROBOT_ID = config['probot_id']



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


@bot.command()
@commands.check(check_user_is_council_or_dev)
@commands.check(check_channel_is_dm)
async def watch(ctx):
    if len(ctx.message.content.split(' ')) < 2:
        return await ctx.send("Usage: `!watch <direct_message_url>`")

    channel = discord.utils.get(bot.get_all_channels(), id=ATTENDANCE_CHANNEL_ID)
    message = await discord.utils.get(channel.history(), jump_url=ctx.message.content[len('!watch'):].strip())
    
    reaction_map = {}
    for reaction in message.reactions:
        reaction_map[reaction.emoji.name] = []
        async for user in reaction.users():
            if user.id == PROBOT_ID:
                continue
            reaction_map[reaction.emoji.name].append(f'{user.name}#{user.discriminator}')

    on_time = 'attcheck'
    early = 'attearly'
    late = 'attlate'
    maybe = 'attmaybe'
    decline = 'attdecline'

    agc = await agcm.authorize()
    ss = await agc.open_by_url(GOOGLE_SHEETS_URL)
    ws = await ss.worksheet("Linkshell Roster")

    # Generate batch-update commands for each user so we don't get rate-limited
    def add_to_batch(batch_updates, user_id, user_id_col_values, val):
        try:
            row = user_id_col_values.index(user_id) + 1
            batch_updates.append({'range': f'W{row}', 'values': [[val]]})
        except:
            pass

    col_values = await ws.col_values(2)
    batch_updates = []
    for user_id in reaction_map['attcheck']:
        add_to_batch(batch_updates, user_id, col_values, 'o')
    
    for user_id in reaction_map['attearly'] + reaction_map['attlate'] + reaction_map['attmaybe']:
        add_to_batch(batch_updates, user_id, col_values, '/')
    
    for user_id in reaction_map['attdecline']:
        add_to_batch(batch_updates, user_id, col_values, 'x')

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
    num_commits = 5
    version_content = subprocess.check_output(['git', 'log', '--use-mailmap', f'-n{num_commits}'])
    await ctx.send("Most recent changes:\n```" + str(version_content, 'utf-8') + "```")


bot.run(BOT_TOKEN)
