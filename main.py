"""
Pro Advanced Group Scheduler Bot
Features:
- Jobs created by replying in group with /s plus optional args:
    /s count=10 delay=5 at=22:30 repeat=daily auto_start=true
- Templates with placeholders: {username}, {first_name}, {date}, {time}
- Inline job cards: Start / Pause / Resume / Stop / Edit / Clone / Details
- RBAC: owner (ENV OWNER_ID), manager/editor/viewer roles
- Logs saved to logs.json and state in state_pro.json
- Export logs as CSV via /exportlogs
- Limits: max_count = 200 per job, per-day-group cap = 1000
- SAFE_MODE: jobs are queued and require Start unless auto_start=true
- NOTE: This is for lawful/educational use only.
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
MAX_COUNT_HARD = 200                 # per-job hard cap (user requested)
PER_DAY_GROUP_CAP = 1000             # per-group per UTC-day cap
DEFAULT_COUNT = 3
DEFAULT_DELAY = 2.0
SAFE_MODE_DEFAULT = True             # require explicit Start by default
# ----------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pro_scheduler")

# -------------- Persistence --------------
def load_state() -> Dict[str, Any]:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            logger.exception("Failed to load STATE_FILE, resetting.")
    # default state
    return {
        "owner_id": None,
        "admins": {},          # "id": "role"
        "templates": [],       # list of {"type":"text"|"photo","content":...,"file_id":...,"tags":[...]}
        "jobs": {},            # job_id -> metadata
        "settings": {
            "max_count": MAX_COUNT_HARD,
            "per_day_group_cap": PER_DAY_GROUP_CAP,
            "safe_mode": SAFE_MODE_DEFAULT
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

# in-memory tasks map to manage running jobs
running_tasks: Dict[str, asyncio.Task] = {}

# -------------- Helpers & RBAC --------------
def is_owner(user_id: int) -> bool:
    owner = os.getenv("OWNER_ID")
    return owner is not None and str(user_id) == str(owner)

def get_role(user_id: int) -> Optional[str]:
    if is_owner(user_id):
        return "owner"
    return state.get("admins", {}).get(str(user_id))

def can_manage(user_id: int) -> bool:
    return get_role(user_id) in ("owner", "manager")

def can_edit(user_id: int) -> bool:
    return get_role(user_id) in ("owner", "manager", "editor")

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

# placeholders
def render_placeholders(text: str, user: Any) -> str:
    now = datetime.utcnow()
    replacements = {
        "{username}": getattr(user, "username", "") or "",
        "{first_name}": getattr(user, "first_name", "") or "",
        "{last_name}": getattr(user, "last_name", "") or "",
        "{date}": now.date().isoformat(),
        "{time}": now.time().strftime("%H:%M:%S"),
    }
    for k, v in replacements.items():
        text = text.replace(k, v)
    return text

# parse args like count=10 delay=5 at=22:30 repeat=daily auto_start=true tags=promo
def parse_kv_args(argstr: str):
    kv = {}
    for part in argstr.split():
        if "=" in part:
            k,v = part.split("=",1)
            kv[k.strip().lower()] = v.strip()
    return kv

# ---------------- Commands ----------------
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Pro Scheduler Bot running. Use /help for commands.")

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "Pro Scheduler Commands:\n"
        "/settemplate -> start recording template (editor+)\n"
        "/done_template -> finish template recording\n"
        "/templates -> list templates\n"
        "/cleartemplates -> owner only\n\n"
        "Create job in group: Reply to user's message and send:\n"
        "/s count=10 delay=5 at=22:30 repeat=daily auto_start=true tags=promo\n"
        "Or just /s to create a queued job with defaults.\n\n"
        "Admin:\n"
        "/addadmin <user_id> <role>\n"
        "/removeadmin <user_id>\n"
        "/listadmins\n\n"
        "Utility:\n"
        "/jobs -> list jobs\n"
        "/exportlogs -> get logs CSV\n"
    )
    await update.message.reply_text(text)

# ---------------- Templates ----------------
async def settemplate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not can_edit(update.effective_user.id):
        await update.message.reply_text("Permission denied. Editor+ required.")
        return
    context.user_data["rec_template"] = True
    await update.message.reply_text("Recording template. Send text or a photo now. Use /done_template when finished.")

async def done_template(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.pop("rec_template", False):
        save_state(state)
        await update.message.reply_text("Template recording saved.")
    else:
        await update.message.reply_text("Not in recording mode.")

async def templates_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not state.get("templates"):
        await update.message.reply_text("No templates saved.")
        return
    lines = []
    for i, t in enumerate(state["templates"],1):
        typ = t.get("type","text")
        tags = ",".join(t.get("tags",[])) if t.get("tags") else ""
        preview = (t.get("content") or t.get("file_id") or "")[:80]
        lines.append(f"{i}. {typ} tags=[{tags}] - {preview}")
    await update.message.reply_text("\n".join(lines))

async def clear_templates(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Only owner can clear templates.")
        return
    state["templates"] = []
    save_state(state)
    await update.message.reply_text("Templates cleared.")

async def media_receiver(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("rec_template"):
        if update.message.photo:
            fid = update.message.photo[-1].file_id
            tags = []
            state.setdefault("templates", []).append({"type":"photo","file_id":fid,"content": update.message.caption or "", "tags": tags})
            await update.message.reply_text("Photo template saved.")
        elif update.message.text:
            # allow optional tag line: tag:promo at start
            text = update.message.text
            tags = []
            if text.startswith("tag:"):
                parts = text.split(None,1)
                if len(parts) > 1:
                    tagpart, rest = parts[0], parts[1]
                    tags = tagpart.replace("tag:","").split(",")
                    text = rest
            state.setdefault("templates", []).append({"type":"text","content":text,"tags":tags})
            await update.message.reply_text("Text template saved.")
        else:
            await update.message.reply_text("Unsupported type for template.")
        save_state(state)
        return
    # update daily_counts if user interacts (helps 'recent interaction' policies if needed)
    # no reply here for normal flow
    return

# ---------------- Admin management ----------------
async def addadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Only owner can add admins.")
        return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /addadmin <user_id> <role>")
        return
    uid = context.args[0]; role = context.args[1]
    if role not in ("manager","editor","viewer"):
        await update.message.reply_text("Role must be manager/editor/viewer")
        return
    state.setdefault("admins", {})[str(uid)] = role
    save_state(state)
    await update.message.reply_text(f"Added admin {uid} role {role}.")
    append_log({"event":"admin_added","by":str(update.effective_user.id),"admin":str(uid),"role":role})

async def removeadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Only owner can remove admins.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /removeadmin <user_id>")
        return
    uid = context.args[0]
    state.get("admins", {}).pop(str(uid), None)
    save_state(state)
    await update.message.reply_text(f"Removed admin {uid}.")
    append_log({"event":"admin_removed","by":str(update.effective_user.id),"admin":str(uid)})

async def listadmins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Only owner can list admins.")
        return
    lines = [f"{k}: {v}" for k,v in state.get("admins", {}).items()]
    await update.message.reply_text("Admins:\n" + ("\n".join(lines) if lines else "None"))

# ---------------- Job card UI ----------------
def job_card_markup(job_id: str):
    kb = [
        [InlineKeyboardButton("Start", callback_data=f"start|{job_id}"),
         InlineKeyboardButton("Pause", callback_data=f"pause|{job_id}"),
         InlineKeyboardButton("Stop", callback_data=f"stop|{job_id}")],
        [InlineKeyboardButton("Clone", callback_data=f"clone|{job_id}"),
         InlineKeyboardButton("Edit", callback_data=f"edit|{job_id}"),
         InlineKeyboardButton("Details", callback_data=f"details|{job_id}")]
    ]
    return InlineKeyboardMarkup(kb)

# parse time HH:MM
def parse_time_hm(s: str) -> Optional[dtime]:
    try:
        hh, mm = s.split(":")
        return dtime(int(hh), int(mm))
    except Exception:
        return None

async def create_job_card(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # /s in group reply
    uid = update.effective_user.id
    role = get_role(uid)
    if role is None and not is_owner(uid):
        await update.message.reply_text("Only configured admins/owner can create jobs.")
        return
    if not update.message.reply_to_message:
        await update.message.reply_text("Reply to a user's message in group and run /s")
        return
    if not state.get("templates"):
        await update.message.reply_text("No templates saved. Use /settemplate first.")
        return

    # parse optional kv args
    args = " ".join(context.args) if context.args else ""
    opts = parse_kv_args(args)

    count = int(opts.get("count", DEFAULT_COUNT))
    delay = float(opts.get("delay", DEFAULT_DELAY))
    raw_at = opts.get("at")               # e.g. 22:30
    repeat = opts.get("repeat")          # daily/hourly/none
    auto_start = str(opts.get("auto_start","false")).lower() in ("1","true","yes")
    tag_filter = opts.get("tags")        # comma separated tags

    # enforce caps
    if count < 1:
        count = DEFAULT_COUNT
    if count > state.get("settings",{}).get("max_count", MAX_COUNT_HARD):
        count = state["settings"]["max_count"]

    if delay < 0.1:
        delay = DEFAULT_DELAY
    if delay > 3600:
        delay = DEFAULT_DELAY

    # filter templates by tags if provided
    templates = state["templates"]
    if tag_filter:
        wanted = set(t.strip() for t in tag_filter.split(","))
        templates = [t for t in templates if set(t.get("tags",[])) & wanted]
        if not templates:
            await update.message.reply_text("No templates match the provided tags.")
            return

    job_id = uuid.uuid4().hex[:8]
    chat = update.effective_chat
    reply_to_msg = update.message.reply_to_message

    # schedule at absolute time if provided
    start_at_iso = None
    if raw_at:
        hm = parse_time_hm(raw_at)
        if hm:
            # schedule next occurrence of hh:mm in UTC (assume raw_at is local same as server UTC — user should be aware)
            now = datetime.utcnow()
            scheduled_dt = datetime.combine(now.date(), hm).replace(tzinfo=timezone.utc)
            if scheduled_dt <= now.replace(tzinfo=timezone.utc):
                scheduled_dt = scheduled_dt + timedelta(days=1)
            start_at_iso = scheduled_dt.isoformat()

    meta = {
        "job_id": job_id,
        "created_by": str(uid),
        "created_at": datetime.utcnow().isoformat(),
        "chat_id": chat.id,
        "reply_to_message_id": reply_to_msg.message_id,
        "target_id": str(reply_to_msg.from_user.id) if reply_to_msg.from_user else None,
        "target_name": (reply_to_msg.from_user.full_name if reply_to_msg.from_user else ""),
        "templates": templates.copy(),
        "count": count,
        "delay": delay,
        "status": "queued",
        "progress": 0,
        "start_at": start_at_iso,   # iso string or None
        "repeat": repeat,
        "auto_start": auto_start
    }
    state.setdefault("jobs", {})[job_id] = meta
    save_state(state)

    text = (f"🎯 Job {job_id}\nTarget: {meta['target_name']}\nCount: {meta['count']} Delay: {meta['delay']}s\n"
            f"Start_at: {meta['start_at'] or 'ASAP'} Repeat: {meta['repeat'] or 'none'}\nStatus: {meta['status']}")
    sent = await update.message.reply_text(text, reply_markup=job_card_markup(job_id))
    state["jobs"][job_id]["card_message_id"] = sent.message_id
    state["jobs"][job_id]["card_chat_id"] = sent.chat_id
    save_state(state)
    append_log({"event":"job_created","job_id":job_id,"by":str(uid),"chat_id":str(chat.id),"meta": {"count":count,"delay":delay,"start_at":start_at_iso}})
    # If auto_start and SAFE_MODE disabled, start immediately
    safe_mode = state.get("settings",{}).get("safe_mode", SAFE_MODE_DEFAULT)
    if meta["auto_start"] and not safe_mode:
        # start immediately
        asyncio.create_task(start_job(job_id, context))

# ---------------- Callback actions ----------------
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
    # Only managers/owner can control execution
    if action in ("start","pause","stop","clone","edit") and not can_manage(user.id):
        await query.edit_message_text("Only managers/owner can manage job execution.")
        return

    if action == "start":
        if job["status"] == "running":
            await query.edit_message_text(f"Job {job_id} already running.")
            return
        # schedule start or immediate
        asyncio.create_task(start_job(job_id, context))
        append_log({"event":"job_start_requested","job_id":job_id,"by":str(user.id)})
        await update_job_card(job_id, context)
        return

    if action == "pause":
        if job["status"] == "running":
            job["status"] = "paused"
            save_state(state)
            append_log({"event":"job_paused","job_id":job_id,"by":str(user.id)})
            await update_job_card(job_id, context)
            return
        await query.edit_message_text("Can only pause a running job.")
        return

    if action == "stop":
        job["status"] = "stopped"
        save_state(state)
        append_log({"event":"job_stopped","job_id":job_id,"by":str(user.id)})
        await update_job_card(job_id, context)
        # cancel running task if exists
        t = running_tasks.get(job_id)
        if t and not t.done():
            t.cancel()
        return

    if action == "clone":
        # create a cloned job with new id but same settings; creator becomes current user
        new_id = uuid.uuid4().hex[:8]
        new_job = job.copy()
        new_job["job_id"] = new_id
        new_job["created_by"] = str(user.id)
        new_job["created_at"] = datetime.utcnow().isoformat()
        new_job["status"] = "queued"
        new_job["progress"] = 0
        state["jobs"][new_id] = new_job
        save_state(state)
        append_log({"event":"job_cloned","from":job_id,"to":new_id,"by":str(user.id)})
        await query.edit_message_text(f"Cloned to Job {new_id}.")
        return

    if action == "edit":
        await query.edit_message_text("Edit not implemented in inline UI. Please edit via GitHub/state file or recreate job with desired args.")
        return

    if action == "details":
        # send brief details
        brief = {
            "job_id": job.get("job_id"),
            "status": job.get("status"),
            "progress": job.get("progress"),
            "count": job.get("count"),
            "delay": job.get("delay"),
            "start_at": job.get("start_at"),
            "repeat": job.get("repeat")
        }
        await query.edit_message_text(f"Details:\n{json.dumps(brief, indent=2)}")
        return

async def update_job_card(job_id: str, context: ContextTypes.DEFAULT_TYPE):
    job = state.get("jobs", {}).get(job_id)
    if not job:
        return
    chat_id = job.get("card_chat_id")
    msg_id = job.get("card_message_id")
    if not chat_id or not msg_id:
        return
    text = (f"🎯 Job {job_id}\nTarget: {job.get('target_name')}\nCount: {job.get('count')} Delay: {job.get('delay')}s\n"
            f"Start_at: {job.get('start_at') or 'ASAP'} Repeat: {job.get('repeat') or 'none'}\n"
            f"Status: {job.get('status')} Progress: {job.get('progress',0)}/{job.get('count')}")
    try:
        await context.bot.edit_message_text(text=text, chat_id=chat_id, message_id=msg_id, reply_markup=job_card_markup(job_id))
    except Exception as e:
        logger.debug("update_job_card edit failed: %s", e)

# ---------------- Job Execution ----------------
async def start_job(job_id: str, context: ContextTypes.DEFAULT_TYPE):
    job = state.get("jobs", {}).get(job_id)
    if not job:
        logger.warning("start_job: job not found %s", job_id)
        return
    # If already running, ignore
    if job.get("status") == "running":
        return
    job["status"] = "queued_to_run"
    save_state(state)
    append_log({"event":"job_start_initiated","job_id":job_id})
    # if start_at present and in future, wait until then
    if job.get("start_at"):
        try:
            start_dt = datetime.fromisoformat(job["start_at"])
            now = datetime.utcnow().replace(tzinfo=timezone.utc)
            if start_dt.tzinfo is None:
                start_dt = start_dt.replace(tzinfo=timezone.utc)
            delay_seconds = (start_dt - now).total_seconds()
            if delay_seconds > 0:
                # wait until start time or until job stopped
                while delay_seconds > 0:
                    # reload job to check stop
                    job = state.get("jobs", {}).get(job_id)
                    if not job or job.get("status") == "stopped":
                        append_log({"event":"job_cancelled_before_start","job_id":job_id})
                        return
                    sleep_for = min(delay_seconds, 30)
                    await asyncio.sleep(sleep_for)
                    now = datetime.utcnow().replace(tzinfo=timezone.utc)
                    delay_seconds = (start_dt - now).total_seconds()
        except Exception:
            logger.exception("Invalid start_at format, starting immediately.")
    # final guard: check per-day group cap
    chat_id = job["chat_id"]
    projected_new = get_daily_count(chat_id) + job.get("count",0)
    if projected_new > state.get("settings",{}).get("per_day_group_cap", PER_DAY_GROUP_CAP):
        job["status"] = "stopped"
        save_state(state)
        append_log({"event":"job_blocked_by_group_cap","job_id":job_id,"chat_id":str(chat_id)})
        # attempt to notify owner/creator
        try:
            await context.bot.send_message(chat_id=int(state.get("owner_id") or job["created_by"]), text=f"Job {job_id} blocked: group daily cap exceeded.")
        except Exception:
            pass
        await update_job_card(job_id, context)
        return

    # run actual worker in background task so callback returns fast
    task = asyncio.create_task(run_job(job_id, context))
    running_tasks[job_id] = task

async def run_job(job_id: str, context: ContextTypes.DEFAULT_TYPE):
    job = state.get("jobs", {}).get(job_id)
    if not job:
        return
    # enforce max_count
    max_count = state.get("settings",{}).get("max_count", MAX_COUNT_HARD)
    total = min(job.get("count",0), max_count)
    job["status"] = "running"
    save_state(state)
    append_log({"event":"job_running","job_id":job_id,"count":total})
    try:
        for i in range(job.get("progress",0), total):
            # reload job to see pause/stop
            job = state.get("jobs", {}).get(job_id)
            if not job or job.get("status") in ("paused","stopped"):
                save_state(state)
                await update_job_card(job_id, context)
                append_log({"event":"job_paused_or_stopped_during_run","job_id":job_id,"status": job.get("status") if job else "missing"})
                return
            # per-message group cap check (stop anywhere if limit reached)
            current_daily = get_daily_count(job["chat_id"])
            if current_daily >= state.get("settings",{}).get("per_day_group_cap", PER_DAY_GROUP_CAP):
                job["status"] = "stopped"
                save_state(state)
                append_log({"event":"job_stopped_group_cap_reached","job_id":job_id})
                await update_job_card(job_id, context)
                return
            tpl = job["templates"][i % len(job["templates"])]
            try:
                # render placeholders using the replied-to user as context if available
                target_user = None
                try:
                    # best-effort: we only have target_id string; cannot fetch user object easily
                    # but we can replace with basic info since we lack full user object
                    # attempt to use the reply_to message info stored? not persisted beyond message id
                    pass
                except Exception:
                    pass
                if tpl["type"] == "text":
                    text = render_placeholders(tpl.get("content",""), update_user_obj_placeholder(job))
                    await context.bot.send_message(chat_id=job["chat_id"], text=text, reply_to_message_id=job["reply_to_message_id"])
                elif tpl["type"] == "photo":
                    caption = render_placeholders(tpl.get("content",""), update_user_obj_placeholder(job))
                    await context.bot.send_photo(chat_id=job["chat_id"], photo=tpl["file_id"], caption=caption, reply_to_message_id=job["reply_to_message_id"])
                else:
                    # fallback
                    text = render_placeholders(tpl.get("content",""), update_user_obj_placeholder(job))
                    await context.bot.send_message(chat_id=job["chat_id"], text=text, reply_to_message_id=job["reply_to_message_id"])
                # increment counters & logs
                increment_daily_count(job["chat_id"], 1)
                append_log({"event":"sent","job_id":job_id,"index":i,"chat_id":str(job["chat_id"])})
            except Exception as e:
                logger.exception("send failed for job %s index %s: %s", job_id, i, e)
                append_log({"event":"send_failed","job_id":job_id,"index":i,"error":str(e)})
            job["progress"] = i+1
            save_state(state)
            await update_job_card(job_id, context)
            await asyncio.sleep(job.get("delay", DEFAULT_DELAY))
    finally:
        job = state.get("jobs", {}).get(job_id)
        if job and job.get("status") != "stopped":
            job["status"] = "finished"
            save_state(state)
            append_log({"event":"job_finished","job_id":job_id})
            await update_job_card(job_id, context)
        running_tasks.pop(job_id, None)
        # handle repeat scheduling if needed
        if job and job.get("repeat") in ("daily","hourly"):
            # compute next start_at
            if job["repeat"] == "daily":
                next_dt = datetime.utcnow().replace(tzinfo=timezone.utc) + timedelta(days=1)
            else:
                next_dt = datetime.utcnow().replace(tzinfo=timezone.utc) + timedelta(hours=1)
            job["start_at"] = next_dt.isoformat()
            job["status"] = "queued"
            job["progress"] = 0
            save_state(state)
            append_log({"event":"job_rescheduled","job_id":job_id,"next_start":job["start_at"]})

# helper to provide a minimal user-like object for placeholders
def update_user_obj_placeholder(job):
    class U:
        username = ""
        first_name = job.get("target_name") or ""
        last_name = ""
    return U()

# ---------------- Utility commands ----------------
async def jobs_list_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    role = get_role(update.effective_user.id)
    if role is None and not is_owner(update.effective_user.id):
        await update.message.reply_text("Not authorized.")
        return
    lines = []
    for jid, j in state.get("jobs", {}).items():
        lines.append(f"{jid}: {j.get('status')} {j.get('progress',0)}/{j.get('count')} target={j.get('target_name')}")
    await update.message.reply_text("\n".join(lines) if lines else "No jobs.")

# export logs as CSV
async def exportlogs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    role = get_role(update.effective_user.id)
    if role is None and not is_owner(update.effective_user.id):
        await update.message.reply_text("Not authorized.")
        return
    if not LOG_FILE.exists():
        await update.message.reply_text("No logs yet.")
        return
    # build CSV in memory
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
            writer.writerow({k: l.get(k,"") for k in keys})
    await update.message.reply_text("Logs exported. Sending file...")
    await update.message.reply_document(document=csv_path.open("rb"))

# ---------------- Wiring & Startup ----------------
def build_app(token: str):
    app = ApplicationBuilder().token(token).build()
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("settemplate", settemplate))
    app.add_handler(CommandHandler("done_template", done_template))
    app.add_handler(CommandHandler("templates", templates_list))
    app.add_handler(CommandHandler("cleartemplates", clear_templates))
    app.add_handler(CommandHandler("addadmin", addadmin))
    app.add_handler(CommandHandler("removeadmin", removeadmin))
    app.add_handler(CommandHandler("listadmins", listadmins))
    app.add_handler(CommandHandler("jobs", jobs_list_cmd))
    app.add_handler(CommandHandler("exportlogs", exportlogs))

    # message receivers: templates & group trigger
    app.add_handler(MessageHandler(filters.PHOTO | (filters.TEXT & ~filters.COMMAND), media_receiver))
    # group trigger: /s or .s
    app.add_handler(MessageHandler(filters.Regex(r'^(/s|\.s)(?:\s|$)'), create_job_card))
    app.add_handler(CallbackQueryHandler(callback_query_handler))
    return app

def main():
    token = os.getenv("TELEGRAM_TOKEN")
    if not token:
        logger.error("TELEGRAM_TOKEN not set. Exiting.")
        return
    owner = os.getenv("OWNER_ID")
    if owner and not state.get("owner_id"):
        state["owner_id"] = str(owner)
        save_state(state)
    logger.info("Starting Pro Advanced Scheduler Bot (polling).")
    app = build_app(token)
    app.run_polling()

if __name__ == "__main__":
    main()
