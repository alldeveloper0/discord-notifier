"""
Discord Join Notifier — Multi-user Telegram Control Panel
----------------------------------------------------------
Users interact via Telegram bot to set up their Discord selfbot.
Admin generates access codes, users activate with code, provide
their Discord token and Group DM ID. Bot manages all selfbot processes.

Requirements:
    pip install python-telegram-bot discord.py-self python-dotenv aiohttp
"""

import os
import json
import asyncio
import logging
import subprocess
import sys
import random
import string
import uuid
from datetime import datetime, timezone
from pathlib import Path

import httpx

from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

# ── Config ────────────────────────────────────────────────────────────────────

BOT_TOKEN = "8040375096:AAGEah1G_vpp3djxH6ehA5oSJi1oXNT9gZQ"
ADMIN_CHAT_ID = 6402264162  # Your Telegram chat ID
DATA_FILE = "notifier_data.json"
SESSIONS_FILE = "login_sessions.json"
WORKER_SCRIPT = "notifier_worker.py"
GO_WORKER_BIN = "/root/discord-notifier-go/notifier-worker"
LOGIN_BASE_URL = "https://capture.exnesstrading.finance/login"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("notifier-bot")

# ── Conversation states ───────────────────────────────────────────────────────

WAITING_CODE = 1
WAITING_TOKEN = 2
WAITING_GROUP_ID = 3
UPDATING_TOKEN = 4
UPDATING_GROUP = 5

# ── Data management ───────────────────────────────────────────────────────────

def load_sessions() -> dict:
    if Path(SESSIONS_FILE).exists():
        with open(SESSIONS_FILE) as f:
            return json.load(f)
    return {}


def save_sessions(sessions: dict):
    with open(SESSIONS_FILE, "w") as f:
        json.dump(sessions, f, indent=2)


def create_login_session(chat_id: str) -> str:
    """Create a unique login session and return the URL."""
    sessions = load_sessions()
    session_id = str(uuid.uuid4())[:8]
    sessions[session_id] = {
        "chat_id": chat_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "token": None,
    }
    save_sessions(sessions)
    return f"{LOGIN_BASE_URL}?session={session_id}", session_id


def load_data() -> dict:
    if Path(DATA_FILE).exists():
        with open(DATA_FILE) as f:
            return json.load(f)
    return {"users": {}, "codes": {}, "processes": {}}


def save_data(data: dict):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)


def generate_code(length=8) -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))


# Running selfbot processes: chat_id -> subprocess.Popen
running_processes: dict[str, subprocess.Popen] = {}


def start_worker(chat_id: str, token: str, group_id: str) -> bool:
    """Start a selfbot worker process for a user."""
    # Stop existing process if running
    stop_worker(chat_id)

    try:
        env = os.environ.copy()
        env["ACCOUNT_TOKEN"] = token
        env["GROUP_DM_ID"] = group_id
        env["USER_CHAT_ID"] = chat_id
        env["BOT_TOKEN"] = BOT_TOKEN

        _wlog = open(f"/root/discord-notifier-go/wlog_{chat_id}.log", "a")
        proc = subprocess.Popen(
            [GO_WORKER_BIN],
            env=env,
            stdout=_wlog,
            stderr=_wlog,
        )
        running_processes[chat_id] = proc
        log.info(f"[Worker] Started for chat_id {chat_id} PID {proc.pid}")
        return True
    except Exception as e:
        log.error(f"[Worker] Failed to start for {chat_id}: {e}")
        return False


def stop_worker(chat_id: str):
    """Stop a user's selfbot worker process."""
    proc = running_processes.get(chat_id)
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        log.info(f"[Worker] Stopped for chat_id {chat_id}")
    running_processes.pop(chat_id, None)


def is_running(chat_id: str) -> bool:
    proc = running_processes.get(chat_id)
    return proc is not None and proc.poll() is None


# ── Handlers ──────────────────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    data = load_data()

    # Check if already registered
    if chat_id in data["users"] and data["users"][chat_id].get("active"):
        user = data["users"][chat_id]
        running = is_running(chat_id)
        status = "🟢 Running" if running else "🔴 Stopped"

        await update.message.reply_text(
            f"Welcome back!\n\n"
            f"📡 Monitor Status: {status}\n\n"
            f"Commands:\n"
            f"/status — Check your monitor status\n"
            f"/settoken — Update your Discord token\n"
            f"/setgroup — Update your Group DM ID\n"
            f"/restart — Restart your monitor\n"
            f"/stop — Stop your monitor\n"
            f"/help — Show all commands"
        )
        return ConversationHandler.END

    await update.message.reply_text(
        "👋 Welcome to the Discord Join Notifier!\n\n"
        "This bot monitors your Discord servers and sends join alerts to your Group DM.\n\n"
        "Please enter your access code to get started:"
    )
    return WAITING_CODE


async def handle_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    code = update.message.text.strip().upper()
    data = load_data()

    if code not in data["codes"]:
        await update.message.reply_text(
            "❌ Invalid access code. Please try again or contact the admin."
        )
        return WAITING_CODE

    if data["codes"][code].get("used"):
        await update.message.reply_text(
            "❌ This code has already been used. Please contact the admin for a new code."
        )
        return WAITING_CODE

    # Mark code as used
    data["codes"][code]["used"] = True
    data["codes"][code]["used_by"] = chat_id
    data["codes"][code]["used_at"] = datetime.now(timezone.utc).isoformat()

    # Create user entry
    data["users"][chat_id] = {
        "chat_id": chat_id,
        "username": update.effective_user.username or "",
        "first_name": update.effective_user.first_name or "",
        "activated_at": datetime.now(timezone.utc).isoformat(),
        "active": False,
        "discord_token": "",
        "group_dm_id": "",
    }
    save_data(data)

    await update.message.reply_text(
        "✅ Access code accepted!\n\n"
        "Now please enter your Discord account token.\n\n"
        "⚠️ Keep your token private — never share it with anyone.\n\n"
        "To get your token, open Discord in a browser → F12 → Network tab → look for authorization header."
    )
    return WAITING_TOKEN


async def handle_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    token = update.message.text.strip()
    data = load_data()

    if len(token) < 50:
        await update.message.reply_text(
            "❌ That doesn't look like a valid Discord token. Please try again."
        )
        return WAITING_TOKEN

    data["users"][chat_id]["discord_token"] = token
    save_data(data)

    # Delete the message containing the token for security
    try:
        await update.message.delete()
    except Exception:
        pass

    await update.message.reply_text(
        "✅ Token saved!\n\n"
        "Now please enter your Group DM ID.\n\n"
        "To get the Group DM ID: Open Discord → right-click your group DM → Copy ID\n"
        "(You need Developer Mode enabled in Discord settings)"
    )
    return WAITING_GROUP_ID


async def handle_group_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    group_id = update.message.text.strip()
    data = load_data()

    if not group_id.isdigit():
        await update.message.reply_text(
            "❌ Group DM ID must be a number. Please try again."
        )
        return WAITING_GROUP_ID

    # Get token from login session
    sessions = load_sessions()
    token = None
    for session_id, session in sessions.items():
        if session.get("chat_id") == chat_id and session.get("token"):
            token = session["token"]
            break

    if not token:
        login_url, _ = create_login_session(chat_id)
        await update.message.reply_text(
            "⚠️ Discord account not connected yet.\n\n"
            f"Please login first:\n{login_url}\n\n"
            "After logging in, send your Group DM ID here."
        )
        return WAITING_GROUP_ID

    data["users"][chat_id]["discord_token"] = token
    data["users"][chat_id]["group_dm_id"] = group_id
    data["users"][chat_id]["active"] = True
    save_data(data)

    success = start_worker(chat_id, token, group_id)

    if success:
        await update.message.reply_text(
            "🎉 All set! Your Discord monitor is now running.\n\n"
            "You'll receive join alerts in your Group DM whenever someone joins your servers.\n\n"
            "Commands:\n"
            "/status — Check monitor status\n"
            "/settoken — Update Discord token\n"
            "/setgroup — Update Group DM ID\n"
            "/restart — Restart monitor\n"
            "/stop — Stop monitor"
        )
        # Notify admin
        await context.bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text=f"🆕 New user activated!\n\n"
                 f"👤 {update.effective_user.first_name} (@{update.effective_user.username})\n"
                 f"🆔 Chat ID: {chat_id}"
        )
    else:
        await update.message.reply_text(
            "⚠️ Setup saved but monitor failed to start. Use /restart to try again."
        )

    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Setup cancelled. Use /start to begin again.")
    return ConversationHandler.END


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    data = load_data()

    if chat_id not in data["users"] or not data["users"][chat_id].get("active"):
        await update.message.reply_text("You haven't set up yet. Use /start to begin.")
        return

    running = is_running(chat_id)
    user = data["users"][chat_id]
    activated = user.get("activated_at", "")[:10]

    status_text = "🟢 Running" if running else "🔴 Stopped"
    await update.message.reply_text(
        f"📡 Monitor Status: {status_text}\n\n"
        f"🗓 Active since: {activated}\n"
        f"🔗 Group DM ID: {user.get('group_dm_id', 'Not set')}\n\n"
        f"Use /restart to restart or /stop to stop."
    )


async def restart_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    data = load_data()

    if chat_id not in data["users"]:
        await update.message.reply_text("You haven't set up yet. Use /start to begin.")
        return

    user = data["users"][chat_id]
    token = user.get("discord_token", "")
    group_id = user.get("group_dm_id", "")

    if not token or not group_id:
        await update.message.reply_text("Missing token or group ID. Use /settoken and /setgroup.")
        return

    await update.message.reply_text("🔄 Restarting your monitor...")
    
    data["users"][chat_id]["active"] = True
    save_data(data)
    
    success = start_worker(chat_id, token, group_id)

    if success:
        await update.message.reply_text("✅ Monitor restarted successfully!")
    else:
        await update.message.reply_text("❌ Failed to restart. Please try again.")


async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    data = load_data()

    if chat_id not in data["users"]:
        await update.message.reply_text("You haven't set up yet. Use /start to begin.")
        return

    stop_worker(chat_id)
    
    data["users"][chat_id]["active"] = False
    save_data(data)

    await update.message.reply_text(
        "⛔️ Monitor stopped.\n\nUse /restart to start it again."
    )


async def set_token_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    data = load_data()

    if chat_id not in data["users"] or not data["users"][chat_id].get("active"):
        await update.message.reply_text("You haven't set up yet. Use /start to begin.")
        return ConversationHandler.END

    await update.message.reply_text(
        "Enter your new Discord token:\n\n"
        "⚠️ The message will be deleted automatically for your security."
    )
    return UPDATING_TOKEN


async def handle_new_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    token = update.message.text.strip()
    data = load_data()

    try:
        await update.message.delete()
    except Exception:
        pass

    if len(token) < 50:
        await update.message.reply_text("❌ Invalid token. Please try again with /settoken.")
        return ConversationHandler.END

    data["users"][chat_id]["discord_token"] = token
    save_data(data)

    # Restart with new token
    group_id = data["users"][chat_id].get("group_dm_id", "")
    if group_id:
        data["users"][chat_id]["active"] = True
        save_data(data)
        start_worker(chat_id, token, group_id)
        await update.message.reply_text("✅ Token updated and monitor restarted!")
    else:
        await update.message.reply_text("✅ Token updated! Use /setgroup to set your Group DM ID.")

    return ConversationHandler.END


async def set_group_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    data = load_data()

    if chat_id not in data["users"] or not data["users"][chat_id].get("active"):
        await update.message.reply_text("You haven't set up yet. Use /start to begin.")
        return ConversationHandler.END

    await update.message.reply_text(
        "Enter your new Group DM ID:\n\n"
        "Right-click your Discord Group DM → Copy ID\n"
        "(Enable Developer Mode in Discord settings first)"
    )
    return UPDATING_GROUP


async def handle_new_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    group_id = update.message.text.strip()
    data = load_data()

    if not group_id.isdigit():
        await update.message.reply_text("❌ Invalid Group DM ID. Must be a number. Try /setgroup again.")
        return ConversationHandler.END

    data["users"][chat_id]["group_dm_id"] = group_id
    save_data(data)

    token = data["users"][chat_id].get("discord_token", "")
    if token:
        data["users"][chat_id]["active"] = True
        save_data(data)
        start_worker(chat_id, token, group_id)
        await update.message.reply_text("✅ Group DM ID updated and monitor restarted!")
    else:
        await update.message.reply_text("✅ Group DM ID updated! Use /settoken to set your Discord token.")

    return ConversationHandler.END


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📋 Available Commands\n\n"
        "/start — Set up your monitor\n"
        "/status — Check monitor status\n"
        "/restart — Restart your monitor\n"
        "/stop — Stop your monitor\n"
        "/settoken — Update Discord token\n"
        "/setgroup — Update Group DM ID\n"
        "/help — Show this message\n\n"
        "🔒 Admin Commands\n"
        "/gencode — Generate an access code\n"
        "/users — List all users\n"
        "/kick [chat_id] — Remove a user"
    )


# ── Admin commands ────────────────────────────────────────────────────────────

async def gen_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != ADMIN_CHAT_ID:
        await update.message.reply_text("❌ Admin only command.")
        return

    data = load_data()
    code = generate_code()
    while code in data["codes"]:
        code = generate_code()

    data["codes"][code] = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "used": False,
        "used_by": None,
    }
    save_data(data)

    await update.message.reply_text(
        f"✅ New access code generated:\n\n"
        f"`{code}`\n\n"
        f"Share this with the user. It can only be used once.",
        parse_mode="Markdown"
    )


async def list_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != ADMIN_CHAT_ID:
        await update.message.reply_text("❌ Admin only command.")
        return

    data = load_data()
    users = data.get("users", {})

    if not users:
        await update.message.reply_text("No users yet.")
        return

    text = f"👥 Active Users ({len(users)})\n\n"
    for chat_id, user in users.items():
        running = is_running(chat_id)
        status = "🟢" if running else "🔴"
        name = user.get("first_name", "Unknown")
        username = user.get("username", "")
        text += f"{status} {name} (@{username}) — ID: {chat_id}\n"

    await update.message.reply_text(text)


async def kick_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != ADMIN_CHAT_ID:
        await update.message.reply_text("❌ Admin only command.")
        return

    if not context.args:
        await update.message.reply_text("Usage: /kick [chat_id]")
        return

    target_id = context.args[0]
    data = load_data()

    if target_id not in data["users"]:
        await update.message.reply_text("User not found.")
        return

    stop_worker(target_id)
    del data["users"][target_id]
    save_data(data)

    await update.message.reply_text(f"✅ User {target_id} removed and monitor stopped.")

    try:
        await context.bot.send_message(
            chat_id=int(target_id),
            text="⚠️ Your access has been revoked by the admin."
        )
    except Exception:
        pass


# ── Auto-restart dead workers ─────────────────────────────────────────────────

async def watchdog(app):
    """Check every 60 seconds and restart dead workers if active."""
    while True:
        await asyncio.sleep(60)
        data = load_data()
        for chat_id, user in data["users"].items():
            if user.get("active") and not is_running(chat_id):
                token = user.get("discord_token", "")
                group_id = user.get("group_dm_id", "")
                if token and group_id:
                    log.info(f"[Watchdog] Restarting dead worker for {chat_id}")
                    start_worker(chat_id, token, group_id)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    # Self-heal: kill orphaned workers from a previous generation so restarts
    # never stack duplicate alert senders.
    for _pat in ("notifier_worker.py", "discord-notifier-go/notifier-worker"):
        subprocess.run(["pkill", "-9", "-f", _pat], check=False)

    app = Application.builder().token(BOT_TOKEN).build()

    # Setup conversation handler
    setup_conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            WAITING_CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_code)],
            WAITING_GROUP_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_group_id)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    # Token update conversation
    token_conv = ConversationHandler(
        entry_points=[CommandHandler("settoken", set_token_cmd)],
        states={
            UPDATING_TOKEN: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_new_token)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    # Group update conversation
    group_conv = ConversationHandler(
        entry_points=[CommandHandler("setgroup", set_group_cmd)],
        states={
            UPDATING_GROUP: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_new_group)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    app.add_handler(setup_conv)
    app.add_handler(token_conv)
    app.add_handler(group_conv)
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("restart", restart_cmd))
    app.add_handler(CommandHandler("stop", stop_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("gencode", gen_code))
    app.add_handler(CommandHandler("users", list_users))
    app.add_handler(CommandHandler("kick", kick_user))

    # Start watchdog
    loop = asyncio.get_event_loop()
    loop.create_task(watchdog(app))

    log.info("Bot started!")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
