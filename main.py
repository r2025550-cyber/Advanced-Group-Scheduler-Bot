
"""
Ultimate Pro Scheduler + Promotion Bot (single-file)

Features:
- Reply in group with /s count=X delay=Y -> creates a job targeting replied user
- Job control card sent ONLY to OWNER's DM (so others in group can't see controls)
- Buttons: Start, Pause, Stop, Progress, Details, Clone, Edit (limited), Delete, Logs, Promotion, Settings
- Promotions: /setpromo (record), /promos (list), /delpromo <id>, /runpromo <id> count=... delay=...
- Commands: /setcount, /setdelay to set defaults
- Limits: max_count per job = 200, per-group-per-day cap = 1000
- Logs saved to logs.json; state saved to state_pro.json
- SAFE MODE default: jobs are queued; must Start from owner's DM unless auto_start and safe_mode off
- Use responsibly. Do not use for unsolicited harassment.

Deploy notes: set TELEGRAM_TOKEN and OWNER_ID (numeric) in environment (Railway variables)
"""
import os
import json
import logging
import asyncio
import uuid
import csv
from pathlib import Path
from datetime import datetime, timezone, timedelta, time as dtime
from typing import Dict, Any, Optional

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler,
    filters, ContextTypes
)

# ---------------- Config ----------------
STATE_FILE = Path("state_pro.json")
LOG_FILE = Path("logs.json")
MAX_COUNT_HARD = 200                 # per-job maximum
PER_DAY_GROUP_CAP = 1000             # per-group per UTC-day cap
DEFAULT_COUNT = 3
DEFAULT_DELAY = 2.0
SAFE_MODE_DEFAULT = True             # require explicit Start by default
OWNER_ENV = "OWNER_ID"
# ----------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ultimate_scheduler")

# ---------------- Persistence ----------------
def load_state() -> Dict[str, Any]:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            logger.exception("Failed to load state file; resetting.")
    return {
        "owner_id": None,
        "admins": {},          # "id": "role"
        "templates": [],       # {"id","type","content","file_id","tags"}
        "promos": [],          # promotions similar to templates with ids
        "jobs": {},            # job_id -> metadata
        "settings": {
            "max_count": MAX_COUNT_HARD,
            "per_day_group_cap": PER_DAY_GROUP_CAP,
            "safe_mode": SAFE_MODE_DEFAULT,
            "default_count": DEFAULT_COUNT,
            "default_delay": DEFAULT_DELAY
        },
        "daily_counts": {}     # "YYYY-MM-DD|chat_id": count
    }

def save_state(s: Dict[str, Any]):
    STATE_FILE.write_text(json.dumps(s, indent=2), encoding="utf-8")

def append_log(entry: Dict[str, Any]):
    logs = []
    if LOG_FILE.exists():
        try:
            logs = json.loads(LOG_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    entry["ts"] = datetime.utcnow().isoformat()
    logs.append(entry)
    LOG_FILE.write_text(json.dumps(logs, indent=2), encoding="utf-8")

state = load_state()

# in-memory running tasks map
running_tasks: Dict[str, asyncio.Task] = {}

# ---------------- Helpers & RBAC ----------------
def owner_env_id() -> Optional[str]:
    return os.getenv(OWNER_ENV)

def is_owner(user_id: int) -> bool:
    owner = owner_env_id()
    return owner is not None and str(user_id) == str(owner)

def get_role(user_id: int) -> Optional[str]:
    if is_owner(user_id):
        return "owner"
    return state.get("admins", {}).get(str(user_id))

def can_manage(user_id: int) -> bool:
    return get_role(user_id) in ("owner", "manager")

def can_edit(user_id: int) -> bool:
    return get_role(user_id) in ("owner", "manager", "editor")

# daily counters helpers
def today_key(chat_id: int) -> str:
    return f"{datetime.utcnow().date().isoformat()}|{chat_id}"

def increment_daily_count(chat_id: int, amount: int = 1) -> int:
    key = today_key(chat_id)
    state.setdefault("daily_counts", {})
    state["daily_counts"][key] = state["daily_counts"].get(key, 0) + amount
    save_state(state)
    return state["daily_counts"][key]

def get_daily_count(chat_id: int) -> int:
    return state.get("daily_counts", {}).get(today_key(chat_id), 0)

# placeholder rendering
def render_placeholders(text: str, job: Dict[str, Any]) -> str:
    now = datetime.utcnow()
    replacements = {
        "{target_name}": job.get("target_name", ""),
        "{username}": ("@" + job.get("target_username")) if job.get("target_username") else "",
        "{first_name}": job.get("target_first_name", "") or "",
        "{last_name}": job.get("target_last_name", "") or "",
        "{date}": now.date().isoformat(),
        "{time}": now.time().strftime("%H:%M:%S"),
    }
    for k, v in replacements.items():
        text = text.replace(k, v)
    return text

# parse args like "count=10 delay=5 at=22:30 repeat=daily auto_start=true tags=promo"
def parse_kv_args(argstr: str):
    kv = {}
    for part in argstr.split():
        if "=" in part:
            k, v = part.split("=", 1)
            kv[k.strip().lower()] = v.strip()
    return kv

def parse_time_hm(s: str) -> Optional[dtime]:
    try:
        hh, mm = s.split(":")
        return dtime(int(hh), int(mm))
    except Exception:
        return None

# ---------------- UI (Job card) ----------------
def job_card_markup(job_id: str):
    kb = [
        [
            InlineKeyboardButton("▶ Start", callback_data=f"start|{job_id}"),
            InlineKeyboardButton("⏸ Pause", callback_data=f"pause|{job_id}"),
            InlineKeyboardButton("⏹ Stop", callback_data=f"stop|{job_id}")
        ],
        [
            InlineKeyboardButton("📊 Progress", callback_data=f"progress|{job_id}"),
            InlineKeyboardButton("📋 Details", callback_data=f"details|{job_id}"),
            InlineKeyboardButton("🗑 Delete", callback_data=f"delete|{job_id}")
        ],
        [
            InlineKeyboardButton("🔄 Clone", callback_data=f"clone|{job_id}"),
            InlineKeyboardButton("✏️ Edit", callback_data=f"edit|{job_id}"),
            InlineKeyboardButton("📑 Logs", callback_data=f"logs|{job_id}")
        ],
        [
            InlineKeyboardButton("📢 Promotion", callback_data=f"promo|{job_id}"),
            InlineKeyboardButton("⚙ Settings", callback_data=f"settings|{job_id}"),
        ]
    ]
    return InlineKeyboardMarkup(kb)

# ---------------- Command Handlers ----------------
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Pro Scheduler Bot running. Use /help to see commands.")

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "Commands:\n"
        "/settemplate - record templates (editor+)\n"
        "/done_template - finish recording\n"
        "/templates - list templates\n"
        "/setpromo - record promotion template (editor+)\n"
        "/promos - list promotions\n"
        "/delpromo <id> - delete promo (owner+)\n"
        "/setcount <n> - set default count (manager+)\n"
        "/setdelay <s> - set default delay (manager+)\n"
        "/s count=.. delay=.. at=HH:MM repeat=daily - reply in group to create job\n"
        "/runpromo <id> count=.. delay=.. - run promotion directly\n        /exportlogs - export logs as CSV (owner)\n"
    )
    await update.message.reply_text(text)

# ---------------- Template Recording ----------------
async def settemplate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not can_edit(update.effective_user.id):
        await update.message.reply_text("Permission denied. Editor+ required.")
        return
    context.user_data["rec_template"] = True
    await update.message.reply_text("Recording template mode ON. Send text or photo. /done_template to finish.")

async def done_template(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.pop("rec_template", False):
        save_state(state)
        await update.message.reply_text("Template(s) saved.")
    else:
        await update.message.reply_text("Not in template recording mode.")

async def templates_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not state["templates"]:
        await update.message.reply_text("No templates.")
        return
    lines = []
    for t in state["templates"]:
        lines.append(f"{t['id']}: {t['type']} - {t.get('content','')[:80]}")
    await update.message.reply_text("\n".join(lines))

async def media_receiver(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Save template if in rec_template mode
    if context.user_data.get("rec_template"):
        if update.message.photo:
            fid = update.message.photo[-1].file_id
            tid = uuid.uuid4().hex[:8]
            state.setdefault("templates", []).append({"id": tid, "type": "photo", "file_id": fid, "content": update.message.caption or "", "tags": []})
            save_state(state)
            await update.message.reply_text(f"Photo template saved (id {tid}).")
        elif update.message.text:
            text = update.message.text
            tid = uuid.uuid4().hex[:8]
            state.setdefault("templates", []).append({"id": tid, "type": "text", "content": text, "tags": []})
            save_state(state)
            await update.message.reply_text(f"Text template saved (id {tid}).")
        else:
            await update.message.reply_text("Unsupported type for template.")
        return
    # Promo recording mode?
    if context.user_data.get("rec_promo"):
        if update.message.photo:
            fid = update.message.photo[-1].file_id
            pid = uuid.uuid4().hex[:8]
            state.setdefault("promos", []).append({"id": pid, "type": "photo", "file_id": fid, "content": update.message.caption or "", "tags": []})
            save_state(state)
            await update.message.reply_text(f"Photo promo saved (id {pid}).")
        elif update.message.text:
            text = update.message.text
            pid = uuid.uuid4().hex[:8]
            state.setdefault("promos", []).append({"id": pid, "type": "text", "content": text, "tags": []})
            save_state(state)
            await update.message.reply_text(f"Promo saved (id {pid}).")
        else:
            await update.message.reply_text("Unsupported type for promo.")
        return
    # otherwise ignore
    return

# ---------------- Promotion System ----------------
async def setpromo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not can_edit(update.effective_user.id):
        await update.message.reply_text("Permission denied. Editor+ required.")
        return
    context.user_data["rec_promo"] = True
    await update.message.reply_text("Recording promo mode ON. Send text/photo. Each message will be a promo (use /done_promo).")

async def done_promo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.pop("rec_promo", False):
        save_state(state)
        await update.message.reply_text("Promos saved.")
    else:
        await update.message.reply_text("Not in promo recording mode.")

async def promos_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not state.get("promos"):
        await update.message.reply_text("No promos.")
        return
    lines = [f"{p['id']}: {p['type']} - {p.get('content','')[:80]}" for p in state["promos"]]
    await update.message.reply_text("\n".join(lines))

async def delpromo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Owner only.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /delpromo <id>")
        return
    pid = context.args[0]
    state["promos"] = [p for p in state.get("promos", []) if p["id"] != pid]
    save_state(state)
    await update.message.reply_text(f"Promo {pid} deleted.")

# ---------------- Settings Commands ----------------
async def setcount_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not can_manage(update.effective_user.id):
        await update.message.reply_text("Manager+ required.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /setcount <n>")
        return
    try:
        n = int(context.args[0])
        if n < 1 or n > state["settings"]["max_count"]:
            await update.message.reply_text(f"Count must be 1..{state['settings']['max_count']}")
            return
        state["settings"]["default_count"] = n
        save_state(state)
        await update.message.reply_text(f"Default count set to {n}.")
    except Exception:
        await update.message.reply_text("Invalid number.")

async def setdelay_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not can_manage(update.effective_user.id):
        await update.message.reply_text("Manager+ required.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /setdelay <seconds>")
        return
    try:
        s = float(context.args[0])
        if s < 0.1 or s > 86400:
            await update.message.reply_text("Delay must be between 0.1 and 86400 seconds.")
            return
        state["settings"]["default_delay"] = s
        save_state(state)
        await update.message.reply_text(f"Default delay set to {s} seconds.")
    except Exception:
        await update.message.reply_text("Invalid value.")

# ---------------- Admin Management ----------------
async def addadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Owner only.")
        return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /addadmin <user_id> <role>")
        return
    uid = context.args[0]; role = context.args[1]
    if role not in ("manager", "editor", "viewer"):
        await update.message.reply_text("Role must be manager/editor/viewer")
        return
    state.setdefault("admins", {})[str(uid)] = role
    save_state(state)
    append_log({"event": "admin_added", "by": str(update.effective_user.id), "admin": str(uid), "role": role})
    await update.message.reply_text(f"Added admin {uid} as {role}.")

async def removeadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Owner only.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /removeadmin <user_id>")
        return
    uid = context.args[0]
    state.get("admins", {}).pop(str(uid), None)
    save_state(state)
    append_log({"event": "admin_removed", "by": str(update.effective_user.id), "admin": str(uid)})
    await update.message.reply_text(f"Removed admin {uid}.")

async def listadmins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Owner only.")
        return
    lines = [f"{k}: {v}" for k, v in state.get("admins", {}).items()]
    await update.message.reply_text("Admins:\n" + ("\n".join(lines) if lines else "None"))

# ---------------- Job Creation (reply in group) ----------------
async def create_job_card(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not (get_role(uid) or is_owner(uid)):
        await update.message.reply_text("You are not authorized to create jobs.")
        return
    if not update.message.reply_to_message:
        await update.message.reply_text("Reply to a user's message in the group, then send /s")
        return
    if not state.get("templates") and not state.get("promos"):
        await update.message.reply_text("No templates or promos saved. Use /settemplate or /setpromo.")
        return

    opts = parse_kv_args(" ".join(context.args)) if context.args else {}
    count = int(opts.get("count", state["settings"]["default_count"]))
    delay = float(opts.get("delay", state["settings"]["default_delay"]))
    raw_at = opts.get("at")
    repeat = opts.get("repeat")
    auto_start = str(opts.get("auto_start", "false")).lower() in ("1", "true", "yes")
    tags = opts.get("tags")

    # enforce caps
    if count < 1:
        count = 1
    if count > state["settings"]["max_count"]:
        count = state["settings"]["max_count"]
    if delay < 0.1:
        delay = state["settings"]["default_delay"]

    # prepare templates set (use all by default, or filter by tags if provided)
    templates = state.get("templates", []).copy()
    if tags:
        wanted = set([t.strip() for t in tags.split(",")])
        templates = [t for t in templates if set(t.get("tags", [])) & wanted]
        if not templates:
            await update.message.reply_text("No templates match tags.")
            return

    # target info
    reply_msg = update.message.reply_to_message
    target_user = reply_msg.from_user
    job_id = uuid.uuid4().hex[:8]

    start_at_iso = None
    if raw_at:
        hm = parse_time_hm(raw_at)
        if hm:
            now = datetime.utcnow().replace(tzinfo=timezone.utc)
            scheduled_dt = datetime.combine(now.date(), hm).replace(tzinfo=timezone.utc)
            if scheduled_dt <= now:
                scheduled_dt += timedelta(days=1)
            start_at_iso = scheduled_dt.isoformat()

    job_meta = {
        "job_id": job_id,
        "created_by": str(uid),
        "created_at": datetime.utcnow().isoformat(),
        "chat_id": update.effective_chat.id,
        "reply_to_message_id": reply_msg.message_id,
        "target_id": str(target_user.id) if target_user else None,
        "target_username": target_user.username if target_user else None,
        "target_first_name": target_user.first_name if target_user else None,
        "target_last_name": target_user.last_name if target_user else None,
        "target_name": target_user.full_name if target_user else "",
        "templates": templates,
        "count": count,
        "delay": delay,
        "status": "queued",
        "progress": 0,
        "start_at": start_at_iso,
        "repeat": repeat,
        "auto_start": auto_start
    }

    state.setdefault("jobs", {})[job_id] = job_meta
    save_state(state)
    append_log({"event": "job_created", "job_id": job_id, "by": str(uid), "chat_id": str(update.effective_chat.id), "meta": {"count": count, "delay": delay}})

    # Notify group minimal (no control buttons)
    await update.message.reply_text("✅ Job created. Controls sent to owner DM.")

    # send job card to owner DM
    owner = owner_env_id()
    if owner:
        try:
            text = (f"🎯 Job {job_id}\nTarget: {job_meta['target_name']}\nCount: {job_meta['count']} Delay: {job_meta['delay']}s\nStart at: {job_meta['start_at'] or 'ASAP'}\nStatus: {job_meta['status']}")
            sent = await context.bot.send_message(chat_id=int(owner), text=text, reply_markup=job_card_markup(job_id))
            # store card location for possible future edits if desired
            state["jobs"][job_id]["card_chat_id"] = sent.chat_id
            state["jobs"][job_id]["card_message_id"] = sent.message_id
            save_state(state)
        except Exception as e:
            logger.exception("Failed to send job card to owner: %s", e)

    # if auto_start and safe_mode disabled, start immediately
    if job_meta["auto_start"] and not state.get("settings", {}).get("safe_mode", SAFE_MODE_DEFAULT):
        asyncio.create_task(start_job(job_id, context))

# ---------------- Callback (buttons) ----------------
async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    parts = data.split("|")
    if len(parts) < 2:
        return
    action, job_id = parts[0], parts[1]
    job = state.get("jobs", {}).get(job_id)
    if not job:
        await query.edit_message_text("Job not found or expired.")
        return
    user = query.from_user

    # manager/owner check for control actions
    if action in ("start", "pause", "stop", "clone", "edit", "delete", "promo", "settings") and not can_manage(user.id) and not is_owner(user.id):
        await query.edit_message_text("Only managers/owner can manage job execution.")
        return

    if action == "start":
        # schedule start on background
        asyncio.create_task(start_job(job_id, context))
        append_log({"event": "job_start_requested", "job_id": job_id, "by": str(user.id)})
        await update_job_card(job_id, context)
        return

    if action == "pause":
        if job["status"] == "running":
            job["status"] = "paused"
            save_state(state)
            append_log({"event": "job_paused", "job_id": job_id, "by": str(user.id)})
            await update_job_card(job_id, context)
            return
        await query.edit_message_text("Can only pause a running job.")
        return

    if action == "stop":
        job["status"] = "stopped"
        save_state(state)
        append_log({"event": "job_stopped", "job_id": job_id, "by": str(user.id)})
        # cancel running task if present
        t = running_tasks.get(job_id)
        if t and not t.done():
            t.cancel()
        await update_job_card(job_id, context)
        return

    if action == "progress":
        prog = job.get("progress", 0)
        cnt = job.get("count", 0)
        percent = (prog / cnt * 100) if cnt else 0
        await query.edit_message_text(f"📊 Job {job_id} Progress:\n{prog}/{cnt} ({percent:.1f}%)", reply_markup=job_card_markup(job_id))
        return

    if action == "details":
        brief = {
            "job_id": job.get("job_id"),
            "status": job.get("status"),
            "progress": job.get("progress"),
            "count": job.get("count"),
            "delay": job.get("delay"),
            "start_at": job.get("start_at"),
            "repeat": job.get("repeat")
        }
        await query.edit_message_text(f"Details:\n{json.dumps(brief, indent=2)}", reply_markup=job_card_markup(job_id))
        return

    if action == "delete":
        state["jobs"].pop(job_id, None)
        save_state(state)
        append_log({"event": "job_deleted", "job_id": job_id, "by": str(user.id)})
        await query.edit_message_text(f"🗑 Job {job_id} deleted.")
        return

    if action == "clone":
        new_id = uuid.uuid4().hex[:8]
        new_job = job.copy()
        new_job["job_id"] = new_id
        new_job["created_by"] = str(user.id)
        new_job["created_at"] = datetime.utcnow().isoformat()
        new_job["status"] = "queued"
        new_job["progress"] = 0
        state["jobs"][new_id] = new_job
        save_state(state)
        append_log({"event": "job_cloned", "from": job_id, "to": new_id, "by": str(user.id)})
        await query.edit_message_text(f"Cloned to Job {new_id}.")
        return

    if action == "edit":
        await query.edit_message_text("Inline edit not implemented. Please recreate the job or edit state file.")
        return

    if action == "logs":
        logs = []
        if LOG_FILE.exists():
            try:
                logs = json.loads(LOG_FILE.read_text(encoding="utf-8"))
            except Exception:
                pass
        jlogs = [l for l in logs if l.get("job_id") == job_id]
        if not jlogs:
            await query.edit_message_text(f"📑 No logs for Job {job_id}")
        else:
            text = "\n".join([f"{x.get('ts','')} - {x.get('event')}" for x in jlogs[-12:]])
            await query.edit_message_text(f"📑 Last logs for Job {job_id}:\n{text}")
        return

    if action == "promo":
        # send promo selection to same owner DM (query from owner button)
        # we'll present inline keyboard of promos
        promos = state.get("promos", [])
        if not promos:
            await query.edit_message_text("No promos available. Add with /setpromo")
            return
        kb = []
        for p in promos:
            kb.append([InlineKeyboardButton(f"{p['id']}: {p.get('content','')[:20]}", callback_data=f"runpromo|{job_id}|{p['id']}")])
        kb.append([InlineKeyboardButton("Cancel", callback_data=f"cancel|{job_id}")])
        await query.edit_message_text("Select promo to run for this job:", reply_markup=InlineKeyboardMarkup(kb))
        return

    if action == "settings":
        s = state.get("settings", {})
        txt = (f"Current settings:\nDefault count: {s.get('default_count')}\nDefault delay: {s.get('default_delay')}s\n"
               f"Max count per job: {s.get('max_count')}\nPer-group/day cap: {s.get('per_day_group_cap')}\nSafe mode: {s.get('safe_mode')}")
        await query.edit_message_text(txt, reply_markup=job_card_markup(job_id))
        return

    # handle runpromo selection (callback pattern: runpromo|jobid|promoid)
    if action == "runpromo":
        # parts length >2 expected
        if len(parts) >= 3:
            _, j, promoid = parts[0], parts[1], parts[2]
            # create a promo-run job using promo content
            prom = next((p for p in state.get("promos", []) if p["id"] == promoid), None)
            if not prom:
                await query.edit_message_text("Promo not found.")
                return
            # create a child job that will run the promo instead of templates
            new_id = uuid.uuid4().hex[:8]
            new_job = job.copy()
            new_job["job_id"] = new_id
            new_job["created_by"] = str(user.id)
            new_job["created_at"] = datetime.utcnow().isoformat()
            new_job["status"] = "queued"
            new_job["progress"] = 0
            new_job["templates"] = [prom]  # use promo as template for this run
            state["jobs"][new_id] = new_job
            save_state(state)
            append_log({"event": "promo_job_created", "from_job": job_id, "new_job": new_id, "promo": promoid, "by": str(user.id)})
            await query.edit_message_text(f"Promo job {new_id} created (run from promo {promoid}). Controls in DM.")
            # DM owner a card for new job as well
            try:
                owner = owner_env_id()
                if owner:
                    text = f"🎯 Promo Job {new_id}\nPromo: {promoid}\nCount: {new_job.get('count')} Delay: {new_job.get('delay')}s\nStatus: queued"
                    sent = await context.bot.send_message(chat_id=int(owner), text=text, reply_markup=job_card_markup(new_id))
                    state["jobs"][new_id]["card_chat_id"] = sent.chat_id
                    state["jobs"][new_id]["card_message_id"] = sent.message_id
                    save_state(state)
            except Exception:
                pass
            return

    if action == "cancel":
        await query.edit_message_text("Cancelled.", reply_markup=job_card_markup(job_id))
        return

# ---------------- Start Job / Execution ----------------
async def start_job(job_id: str, context: ContextTypes.DEFAULT_TYPE):
    job = state.get("jobs", {}).get(job_id)
    if not job:
        logger.warning("start_job: job not found %s", job_id)
        return
    if job.get("status") == "running":
        return
    job["status"] = "queued_to_run"
    save_state(state)
    append_log({"event": "job_start_initiated", "job_id": job_id})
    # wait until start_at if provided
    if job.get("start_at"):
        try:
            start_dt = datetime.fromisoformat(job["start_at"])
            now = datetime.utcnow().replace(tzinfo=timezone.utc)
            if start_dt.tzinfo is None:
                start_dt = start_dt.replace(tzinfo=timezone.utc)
            delay_seconds = (start_dt - now).total_seconds()
            if delay_seconds > 0:
                while delay_seconds > 0:
                    job = state.get("jobs", {}).get(job_id)
                    if not job or job.get("status") == "stopped":
                        append_log({"event": "job_cancelled_before_start", "job_id": job_id})
                        return
                    sleep_for = min(delay_seconds, 30)
                    await asyncio.sleep(sleep_for)
                    now = datetime.utcnow().replace(tzinfo=timezone.utc)
                    delay_seconds = (start_dt - now).total_seconds()
        except Exception:
            logger.exception("Invalid start_at format; starting immediately.")
    # check per-day group cap
    chat_id = job["chat_id"]
    projected_new = get_daily_count(chat_id) + job.get("count", 0)
    if projected_new > state.get("settings", {}).get("per_day_group_cap", PER_DAY_GROUP_CAP):
        job["status"] = "stopped"
        save_state(state)
        append_log({"event": "job_blocked_by_group_cap", "job_id": job_id, "chat_id": str(chat_id)})
        try:
            owner = owner_env_id()
            if owner:
                await context.bot.send_message(chat_id=int(owner), text=f"Job {job_id} blocked: group daily cap exceeded.")
        except Exception:
            pass
        await update_job_card(job_id, context)
        return

    # start background worker
    task = asyncio.create_task(run_job(job_id, context))
    running_tasks[job_id] = task

async def run_job(job_id: str, context: ContextTypes.DEFAULT_TYPE):
    job = state.get("jobs", {}).get(job_id)
    if not job:
        return
    max_count = state.get("settings", {}).get("max_count", MAX_COUNT_HARD)
    total = min(job.get("count", 0), max_count)
    job["status"] = "running"
    save_state(state)
    append_log({"event": "job_running", "job_id": job_id, "count": total})
    try:
        for i in range(job.get("progress", 0), total):
            job = state.get("jobs", {}).get(job_id)
            if not job or job.get("status") in ("paused", "stopped"):
                save_state(state)
                await update_job_card(job_id, context)
                append_log({"event": "job_paused_or_stopped", "job_id": job_id, "status": job.get("status") if job else "missing"})
                return
            # per-message group cap check
            current_daily = get_daily_count(job["chat_id"])
            if current_daily >= state.get("settings", {}).get("per_day_group_cap", PER_DAY_GROUP_CAP):
                job["status"] = "stopped"
                save_state(state)
                append_log({"event": "job_stopped_group_cap_reached", "job_id": job_id})
                await update_job_card(job_id, context)
                return
            tpl = job["templates"][i % len(job["templates"])] if job.get("templates") else None
            try:
                if tpl:
                    rendered = tpl.get("content", "")
                    rendered = render_placeholders(rendered, job)
                    if tpl["type"] == "text":
                        await context.bot.send_message(chat_id=job["chat_id"], text=rendered, reply_to_message_id=job["reply_to_message_id"])
                    elif tpl["type"] == "photo":
                        await context.bot.send_photo(chat_id=job["chat_id"], photo=tpl["file_id"], caption=rendered, reply_to_message_id=job["reply_to_message_id"])
                    else:
                        await context.bot.send_message(chat_id=job["chat_id"], text=rendered, reply_to_message_id=job["reply_to_message_id"])
                else:
                    # no template: skip
                    await context.bot.send_message(chat_id=job["chat_id"], text=f"[No template] (job {job_id})", reply_to_message_id=job["reply_to_message_id"])
                # increment daily count & log
                increment_daily_count(job["chat_id"], 1)
                append_log({"event": "sent", "job_id": job_id, "index": i, "chat_id": str(job["chat_id"])})
            except Exception as e:
                logger.exception("send failed for job %s index %s: %s", job_id, i, e)
                append_log({"event": "send_failed", "job_id": job_id, "index": i, "error": str(e)})
            job["progress"] = i + 1
            save_state(state)
            await update_job_card(job_id, context)
            await asyncio.sleep(job.get("delay", DEFAULT_DELAY))
    finally:
        job = state.get("jobs", {}).get(job_id)
        if job and job.get("status") != "stopped":
            job["status"] = "finished"
            save_state(state)
            append_log({"event": "job_finished", "job_id": job_id})
            await update_job_card(job_id, context)
        running_tasks.pop(job_id, None)
        # handle repeat scheduling
        if job and job.get("repeat") in ("daily", "hourly"):
            if job["repeat"] == "daily":
                next_dt = datetime.utcnow().replace(tzinfo=timezone.utc) + timedelta(days=1)
            else:
                next_dt = datetime.utcnow().replace(tzinfo=timezone.utc) + timedelta(hours=1)
            job["start_at"] = next_dt.isoformat()
            job["status"] = "queued"
            job["progress"] = 0
            save_state(state)
            append_log({"event": "job_rescheduled", "job_id": job_id, "next_start": job["start_at"]})

# ---------------- Update job card in owner's DM ----------------
async def update_job_card(job_id: str, context: ContextTypes.DEFAULT_TYPE):
    job = state.get("jobs", {}).get(job_id)
    if not job:
        return
    chat_id = job.get("card_chat_id") or owner_env_id()
    msg_id = job.get("card_message_id")
    if not chat_id or not msg_id:
        return
    text = (f"🎯 Job {job_id}\nTarget: {job.get('target_name')}\nCount: {job.get('count')} Delay: {job.get('delay')}s\n"
            f"Start_at: {job.get('start_at') or 'ASAP'} Repeat: {job.get('repeat') or 'none'}\n"
            f"Status: {job.get('status')} Progress: {job.get('progress', 0)}/{job.get('count')}")
    try:
        await context.bot.edit_message_text(text=text, chat_id=int(chat_id), message_id=msg_id, reply_markup=job_card_markup(job_id))
    except Exception as e:
        logger.debug("update_job_card edit failed: %s", e)

# ---------------- Direct runpromo command ----------------
async def runpromo_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not (get_role(update.effective_user.id) or is_owner(update.effective_user.id)):
        await update.message.reply_text("Not authorized.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /runpromo <promo_id> [count=.. delay=..]")
        return
    promo_id = context.args[0]
    prom = next((p for p in state.get("promos", []) if p["id"] == promo_id), None)
    if not prom:
        await update.message.reply_text("Promo not found.")
        return
    opts = parse_kv_args(" ".join(context.args[1:])) if len(context.args) > 1 else {}
    count = int(opts.get("count", state["settings"]["default_count"]))
    delay = float(opts.get("delay", state["settings"]["default_delay"]))
    if count > state["settings"]["max_count"]:
        count = state["settings"]["max_count"]
    # create a job in same chat if used in group, otherwise return
    if update.effective_chat.type in ("group", "supergroup"):
        target_chat = update.effective_chat.id
        job_id = uuid.uuid4().hex[:8]
        job = {
            "job_id": job_id,
            "created_by": str(update.effective_user.id),
            "created_at": datetime.utcnow().isoformat(),
            "chat_id": target_chat,
            "reply_to_message_id": None,
            "target_id": None,
            "target_name": "",
            "templates": [prom],
            "count": count,
            "delay": delay,
            "status": "queued",
            "progress": 0,
            "start_at": None,
            "repeat": None,
            "auto_start": True
        }
        state["jobs"][job_id] = job
        save_state(state)
        append_log({"event": "promo_direct_created", "job_id": job_id, "promo": promo_id, "by": str(update.effective_user.id)})
        await update.message.reply_text("Promo job created and will be auto-started (controls in owner DM).")
        # DM owner with card
        owner = owner_env_id()
        if owner:
            try:
                text = f"🎯 Promo Job {job_id}\nPromo: {promo_id}\nCount: {count} Delay: {delay}s\nStatus: queued"
                sent = await context.bot.send_message(chat_id=int(owner), text=text, reply_markup=job_card_markup(job_id))
                state["jobs"][job_id]["card_chat_id"] = sent.chat_id
                state["jobs"][job_id]["card_message_id"] = sent.message_id
                save_state(state)
            except Exception:
                pass
        # start job immediately
        asyncio.create_task(start_job(job_id, context))
    else:
        await update.message.reply_text("Run this command from a group to broadcast promo there.")

# ---------------- Logs export ----------------
async def exportlogs_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Owner only.")
        return
    if not LOG_FILE.exists():
        await update.message.reply_text("No logs yet.")
        return
    logs = json.loads(LOG_FILE.read_text(encoding="utf-8"))
    csv_path = Path("logs_export.csv")
    keys = set()
    for l in logs:
        keys.update(l.keys())
    keys = sorted(keys)
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for l in logs:
            writer.writerow({k: l.get(k, "") for k in keys})
    await update.message.reply_text("Logs exported. Sending file...")
    await update.message.reply_document(document=csv_path.open("rb"))

# ---------------- Jobs list command ----------------
async def jobs_list_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    role = get_role(update.effective_user.id)
    if role is None and not is_owner(update.effective_user.id):
        await update.message.reply_text("Not authorized.")
        return
    lines = []
    for jid, j in state.get("jobs", {}).items():
        lines.append(f"{jid}: status={j.get('status')} progress={j.get('progress',0)}/{j.get('count')} target={j.get('target_name')}")
    await update.message.reply_text("\n".join(lines) if lines else "No jobs.")

# ---------------- Wiring ----------------
def build_app(token: str):
    app = ApplicationBuilder().token(token).build()
    # Core
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    # Templates & promos
    app.add_handler(CommandHandler("settemplate", settemplate))
    app.add_handler(CommandHandler("done_template", done_template))
    app.add_handler(CommandHandler("templates", templates_list))
    app.add_handler(CommandHandler("setpromo", setpromo))
    app.add_handler(CommandHandler("done_promo", done_promo))
    app.add_handler(CommandHandler("promos", promos_list))
    app.add_handler(CommandHandler("delpromo", delpromo))
    # Settings & admin
    app.add_handler(CommandHandler("setcount", setcount_cmd))
    app.add_handler(CommandHandler("setdelay", setdelay_cmd))
    app.add_handler(CommandHandler("addadmin", addadmin))
    app.add_handler(CommandHandler("removeadmin", removeadmin))
    app.add_handler(CommandHandler("listadmins", listadmins))
    # Jobs & promos run
    app.add_handler(CommandHandler("s", create_job_card))
    app.add_handler(CommandHandler("runpromo", runpromo_cmd))
    app.add_handler(CommandHandler("jobs", jobs_list_cmd))
    app.add_handler(CommandHandler("exportlogs", exportlogs_cmd))
    # receivers
    app.add_handler(MessageHandler(filters.PHOTO | (filters.TEXT & ~filters.COMMAND), media_receiver))
    app.add_handler(CallbackQueryHandler(callback_query_handler))
    return app

def main():
    token = os.getenv("TELEGRAM_TOKEN")
    if not token:
        logger.error("TELEGRAM_TOKEN not set. Exiting.")
        return
    # set owner in state if provided
    owner = owner_env_id()
    if owner and not state.get("owner_id"):
        state["owner_id"] = str(owner)
        save_state(state)
    logger.info("Starting Ultimate Pro Scheduler Bot (polling).")
    app = build_app(token)
    app.run_polling()

if __name__ == "__main__":
    main()
