"""
Discord Join Notifier Worker
-----------------------------
One instance runs per user. Spawned by discord_notifier_bot.py.
Watches all servers the Discord account is in and posts join alerts
to the specified Group DM.
"""

import os
import logging
from datetime import datetime, timezone

import discord
from dotenv import load_dotenv

load_dotenv()

ACCOUNT_TOKEN = os.getenv("ACCOUNT_TOKEN")
GROUP_DM_ID   = os.getenv("GROUP_DM_ID")
USER_CHAT_ID  = os.getenv("USER_CHAT_ID", "unknown")

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s [Worker-{USER_CHAT_ID}] %(message)s",
)
log = logging.getLogger("notifier-worker")

client = discord.Client(self_bot=True)


@client.event
async def on_ready():
    log.info(f"Logged in as {client.user} | Watching {len(client.guilds)} server(s)")
    for g in client.guilds:
        log.info(f"  - {g.name} ({g.member_count} members)")


@client.event
async def on_resumed():
    log.info("Gateway reconnected")


@client.event
async def on_disconnect():
    log.info("Gateway disconnected — will auto-reconnect")


@client.event
async def on_member_join(member: discord.Member):
    if member.bot:
        return

    guild = member.guild
    now = datetime.now(timezone.utc)
    account_age_days = (now - member.created_at.replace(tzinfo=timezone.utc)).days
    years = account_age_days // 365
    days = account_age_days % 365
    age_str = f"{years}y {days}d" if years > 0 else f"{days}d"
    join_time = now.strftime("%d %b %Y, %I:%M %p UTC")

    text = (
        f"🔔 New Member Joined\n\n"
        f"👤 {member.name}\n"
        f"🏠 {guild.name}\n"
        f"📅 Account Age: {age_str}\n"
        f"🕐 Joined: {join_time}"
    )

    log.info(f"Join: {member.name} in {guild.name}")

    if not GROUP_DM_ID:
        return

    target = client.get_channel(int(GROUP_DM_ID))
    if target is None:
        log.warning("Group DM channel not found")
        return

    try:
        await target.send(text)
    except discord.Forbidden:
        log.warning("No permission to send in group DM")
    except discord.HTTPException as e:
        log.warning(f"Failed to send: {e}")


if __name__ == "__main__":
    if not ACCOUNT_TOKEN:
        raise SystemExit("ACCOUNT_TOKEN missing")
    client.run(ACCOUNT_TOKEN, reconnect=True)
