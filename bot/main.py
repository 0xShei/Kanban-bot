import os
from dotenv import load_dotenv
import discord
from discord import app_commands
from discord.ext import commands

#.env
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")

# Intents
intents = discord.Intents.default()
intents.message_content = True  

# Intents
class Bot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix='!', intents=intents)

    async def setup_hook(self):
        # Bot ready sync
        await self.tree.sync()
        print(f'Synced slash commands for {self.user}')

bot = Bot()

# /ping
@bot.tree.command(name="ping", description="Check the bot's latency")
async def ping_slash(interaction: discord.Interaction):
    latency = round(bot.latency * 1000)  # latency in milliseconds
    await interaction.response.send_message(f'🏓 Pong! Latency: `{latency}ms`')

@bot.event
async def on_ready():
    print(f'Logged in as {bot.user.name}')
    print(f'Bot ID: {bot.user.id}')
    print('----------------------------------------------------------------------')

# Run
if __name__ == "__main__":
    bot.run(TOKEN)