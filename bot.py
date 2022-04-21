import discord
from discord.ext import commands

import argparse
import yaml
import logging

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
FEEDBACK_CHANNEL_ID = config['feedback_channel_id']

intents = discord.Intents.default()
intents.messages = True
bot = commands.Bot(command_prefix='!', intents=intents)

SUGGESTION_TEMPLATE = '''
**I've got a new suggestion to pass on!**

>>> {}
'''
SUGGESTION_LENGTH_MINIMUM = 20



@bot.command()
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

bot.run(BOT_TOKEN)

