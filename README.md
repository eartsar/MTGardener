# MTGardener

### What is this?

MTGardener is a personal assistant bot for use in the [Eden](https://edenxi.com/) LS MotherTree's discord. The bot is meant to be a lightweight assistant that automates, or enhances, some of the clerical work done. MTGardener can currently do the following things.
- `!attupdate` **[role required]** Update the attendance column on the "roster" page of the master Google Sheet based on the responses to the latest attendance poll.
- `!job` Check the job assigned for a user's characters to the next event.
- `!alertjobs` **[role required]** Send users (opt-in) a DM akin to `!job`.
- `!suggest <suggestion>` Facilitate anonymous suggestions by passing them along to a designated channel, and opening up a thread for discussion.
- `!ping` Check that the bot is up and running.
- `!changelog` Check the changelog for the last few updates (pulled from this repo's history).  


### How do I run it?

1. MTGardener requires Python 3.8+ and uses [Poetry](https://python-poetry.org/) to manage the appropriate environment to run. Install the necessary dependencies by running `poetry install`.
2. Make a copy of the configuration file (`config.yml`) and fill it out. To get the necessary IDs, enable developer mode on your discord client, and right-click the object in question, and select copy ID.
3. Start the bot by running `poetry run python bot.py --config my_config.yml` 
