# Advanced Group Scheduler Bot (Telegram-only)

## ✨ Features
- Inline job cards with **Start / Pause / Stop / Details** buttons.
- Roles: **owner** (set in ENV `OWNER_ID`) and admins with roles (manager/editor/viewer).
- Template recording for **text + photo** messages.
- Jobs post messages **in the same group**, replying to the target’s message.
- Logs saved to `logs.json` for accountability.
- SAFE_MODE default: jobs are only queued, must press **Start** button to actually run.

## 🚀 Deploy (Railway)
1. Create a new Railway project.
2. Upload this folder (or push to GitHub and connect).
3. Add environment variables:
   - `TELEGRAM_TOKEN` = your bot token from BotFather
   - `OWNER_ID` = your Telegram numeric user id
4. Railway auto-detects Python.  
   Start command: `python main.py`
5. Add bot to your group.

## 🛠 Usage
- `/settemplate` → enter recording mode → send text/photo → `/done_template`
- `/templates` → list templates
- In group: **reply** to someone’s message and send `/s` or `.s`  
  → job card appears (with Start/Pause/Stop buttons)
- Owner can `/addadmin <id> <role>` (roles: manager/editor/viewer)

## ⚠️ Important
This bot is for **educational / moderation / demo purposes only**.  
Do **not** use for harassment or unsolicited spam — risk of account ban.
