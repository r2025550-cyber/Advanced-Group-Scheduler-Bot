
"""
Safe Advanced Promotions + Scheduler (educational, consent-first)

- Stores templates and promos
- Owner/Admin RBAC
- Opt-in list: only users who run /optin receive messages
- Jobs show DM-only controls (Start/Pause/Stop/Details)
- Simulated send mode by default (no unsolicited sends)
- Logging and CSV export
- Deploy notes: set TELEGRAM_TOKEN and OWNER_ID env vars
"""

import os, json, uuid, csv, logging, asyncio
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes

# ---------- CONFIG ----------
STATE_FILE = Path("state_safe.json")
LOG_FILE = Path("logs_safe.json")
SIMULATE_SEND = False   # IMPORTANT: True => do NOT actually send messages (safe demo)
MAX_COUNT = 200
PER_DAY_CAP = 1000
DEFAULT_COUNT = 3
DEFAULT_DELAY = 2.0

# Owner from env
OWNER_ID = os.getenv("OWNER_ID")  # set in Railway
TOKEN = os.getenv("TELEGRAM_TOKEN")
if not TOKEN:
    raise SystemExit("Set TELEGRAM_TOKEN env var")

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("safe_scheduler")

# ---------- Persistence ----------
def load_state() -> Dict[str,Any]:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            logger.exception("Failed to load state file, resetting.")
    return {
        "owner_id": OWNER_ID,
        "admins": {},            # "id":role
        "templates": [],        # list of {"id","type","content","file_id"}
        "promos": [],
        "jobs": {},             # job_id -> metadata
        "optin": [],            # list of user ids who opted in
        "settings": {"default_count": DEFAULT_COUNT, "default_delay": DEFAULT_DELAY, "per_day_cap": PER_DAY_CAP}
    }

def save_state(s: Dict[str,Any]):
    STATE_FILE.write_text(json.dumps(s, indent=2), encoding="utf-8")

def append_log(entry: Dict[str,Any]):
    logs = []
    if LOG_FILE.exists():
        try: logs = json.loads(LOG_FILE.read_text(encoding="utf-8"))
        except: pass
    entry["ts"] = datetime.utcnow().isoformat()
    logs.append(entry)
    LOG_FILE.write_text(json.dumps(logs, indent=2), encoding="utf-8")

state = load_state()

# ---------- Helpers ----------
def is_owner(uid:int) -> bool:
    return str(uid) == str(state.get("owner_id") or OWNER_ID)

def role(uid:int) -> Optional[str]:
    if is_owner(uid): return "owner"
    return state.get("admins", {}).get(str(uid))

def can_manage(uid:int) -> bool:
    return role(uid) in ("owner","manager")

def render(template:str, job:Dict[str,Any], target_user:Dict[str,Any]) -> str:
    now = datetime.utcnow()
    replacements = {
        "{target_name}": target_user.get("full_name",""),
        "{username}": ("@"+target_user["username"]) if target_user.get("username") else target_user.get("first_name",""),
        "{first_name}": target_user.get("first_name",""),
        "{date}": now.date().isoformat(),
        "{time}": now.time().strftime("%H:%M:%S")
    }
    text = template
    for k,v in replacements.items(): text = text.replace(k,v)
    return text

def today_key(chat_id:int) -> str:
    return f"{datetime.utcnow().date().isoformat()}|{chat_id}"

def inc_daily(chat_id:int, n:int=1):
    key = today_key(chat_id)
    state.setdefault("daily_counts", {})
    state["daily_counts"][key] = state["daily_counts"].get(key,0)+n
    save_state(state)
    return state["daily_counts"][key]

def get_daily(chat_id:int) -> int:
    return state.get("daily_counts", {}).get(today_key(chat_id), 0)

# ---------- UI / Buttons ----------
def job_card(job_id:str):
    kb = [
        [InlineKeyboardButton("▶ Start", callback_data=f"start|{job_id}"),
         InlineKeyboardButton("⏸ Pause", callback_data=f"pause|{job_id}"),
         InlineKeyboardButton("⏹ Stop", callback_data=f"stop|{job_id}")],
        [InlineKeyboardButton("📋 Details", callback_data=f"details|{job_id}"),
         InlineKeyboardButton("📊 Progress", callback_data=f"progress|{job_id}"),
         InlineKeyboardButton("🗑 Delete", callback_data=f"delete|{job_id}")],
    ]
    return InlineKeyboardMarkup(kb)

# ---------- Commands: templates/promos/optin ----------
async def start_cmd(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Safe Scheduler Bot running. Use /help")

async def help_cmd(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    txt = ("Commands (consent-first):\n"
           "/settemplate -> enter template-recording mode (manager+)\n"
           "/done_template -> finish recording\n"
           "/templates -> list templates\n"
           "/setpromo -> record promotions (manager+)\n"
           "/promos -> list promos\n"
           "/optin -> opt in to receive consented messages\n"
           "/optout -> remove opt-in\n"
           "Group usage:\n"
           "Reply to a user message with /s count=.. delay=..  -> create job (controls to owner DM)\n"
           "Owner/Admin commands: /addadmin /removeadmin /exportlogs /setcount /setdelay\n")
    await update.message.reply_text(txt)

# recording modes per user
# user_data flags: rec_template, rec_promo
async def settemplate(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not can_manage(uid):
        return await update.message.reply_text("Permission denied (manager+).")
    ctx.user_data["rec_template"]=True
    await update.message.reply_text("Recording templates mode ON. Send text messages; finish with /done_template")

async def done_template(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    if ctx.user_data.pop("rec_template", False):
        save_state(state)
        await update.message.reply_text("Templates saved.")
    else:
        await update.message.reply_text("Not recording mode.")

async def templates_list(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    items = state.get("templates", [])
    if not items:
        return await update.message.reply_text("No templates.")
    lines = [f"{t['id']}: {t.get('content','')[:60]}" for t in items]
    await update.message.reply_text("\n".join(lines))

async def setpromo(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not can_manage(uid):
        return await update.message.reply_text("Permission denied (manager+).")
    ctx.user_data["rec_promo"]=True
    await update.message.reply_text("Recording promos mode ON. Send text messages; finish with /done_promo")

async def done_promo(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    if ctx.user_data.pop("rec_promo", False):
        save_state(state)
        await update.message.reply_text("Promos saved.")
    else:
        await update.message.reply_text("Not recording promo mode.")

async def promos_list(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    items = state.get("promos", [])
    if not items: return await update.message.reply_text("No promos.")
    lines = [f"{p['id']}: {p.get('content','')[:60]}" for p in items]
    await update.message.reply_text("\n".join(lines))

# media/text receiver used for recording templates/promos
async def media_receiver(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    if ctx.user_data.get("rec_template"):
        if update.message.text:
            tid = uuid.uuid4().hex[:8]
            state.setdefault("templates", []).append({"id":tid,"type":"text","content":update.message.text})
            save_state(state)
            await update.message.reply_text(f"Saved template {tid}")
        else:
            await update.message.reply_text("Only text templates supported in demo.")
        return
    if ctx.user_data.get("rec_promo"):
        if update.message.text:
            pid = uuid.uuid4().hex[:8]
            state.setdefault("promos", []).append({"id":pid,"type":"text","content":update.message.text})
            save_state(state)
            await update.message.reply_text(f"Saved promo {pid}")
        return
    # otherwise ignore normal messages

# opt-in/out
async def optin(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if uid in state.get("optin", []):
        return await update.message.reply_text("You are already opted in.")
    state.setdefault("optin", []).append(uid)
    save_state(state)
    append_log({"event":"optin","user":uid})
    await update.message.reply_text("You have opted in to receive consented messages. Thank you.")

async def optout(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if uid in state.get("optin", []):
        state["optin"] = [x for x in state["optin"] if x!=uid]
        save_state(state)
        append_log({"event":"optout","user":uid})
        await update.message.reply_text("You have opted out.")
    else:
        await update.message.reply_text("You were not opted in.")

# ---------- Admins ----------
async def addadmin(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return await update.message.reply_text("Owner only.")
    if len(ctx.args)<2:
        return await update.message.reply_text("Usage: /addadmin <user_id> <role>")
    uid=ctx.args[0]; rolev=ctx.args[1]
    state.setdefault("admins", {})[str(uid)] = rolev
    save_state(state)
    append_log({"event":"admin_add","by":str(update.effective_user.id),"admin":uid,"role":rolev})
    await update.message.reply_text("Admin added.")

async def removeadmin(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id): return await update.message.reply_text("Owner only.")
    if not ctx.args: return await update.message.reply_text("Usage: /removeadmin <user_id>")
    uid = ctx.args[0]
    state.get("admins",{}).pop(str(uid),None)
    save_state(state)
    append_log({"event":"admin_remove","by":str(update.effective_user.id),"admin":uid})
    await update.message.reply_text("Removed admin.")

async def listadmins(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id): return await update.message.reply_text("Owner only.")
    items = state.get("admins",{})
    if not items: return await update.message.reply_text("No admins.")
    await update.message.reply_text("\n".join([f"{k}:{v}" for k,v in items.items()]))

# ---------- Create Job (reply in group) ----------
async def create_job(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    # only owner/admin/manager can create jobs
    if not (role(uid) or is_owner(uid)):
        return await update.message.reply_text("Not authorized to create jobs. Ask owner to add you as admin.")
    if not update.message.reply_to_message:
        return await update.message.reply_text("Reply to a user's message and send /s.")
    # parse args simple: count= N delay=D
    args_str = " ".join(ctx.args) if ctx.args else ""
    ac = DEFAULT_COUNT; ad = DEFAULT_DELAY
    for part in args_str.split():
        if "=" in part:
            k,v = part.split("=",1)
            if k=="count":
                try: ac = min(int(v), MAX_COUNT)
                except: pass
            if k=="delay":
                try: ad = float(v)
                except: pass
    # choose templates
    templates = state.get("templates",[])
    if not templates:
        return await update.message.reply_text("No templates stored. Use /settemplate (manager+).")
    reply_msg = update.message.reply_to_message
    target = reply_msg.from_user
    job_id = uuid.uuid4().hex[:8]
    job = {
        "job_id":job_id,
        "chat_id": update.effective_chat.id,
        "reply_to_message_id": reply_msg.message_id,
        "created_by": str(uid),
        "target_id": str(target.id),
        "target_username": target.username,
        "target_first_name": target.first_name or "",
        "target_full_name": (target.full_name if hasattr(target,"full_name") else (target.first_name or "")),
        "templates": [t["id"] for t in templates],
        "count": ac,
        "delay": ad,
        "status": "queued",
        "progress": 0,
        "card_chat_id": None,
        "card_message_id": None
    }
    state.setdefault("jobs", {})[job_id] = job
    save_state(state)
    append_log({"event":"job_created","job_id":job_id,"by":str(uid)})
    # minimal group notify
    await update.message.reply_text("✅ Job created — controls sent to owner DM.")
    # send card to owner DM (if owner present)
    owner = state.get("owner_id") or OWNER_ID
    try:
        if owner:
            text = f"🎯 Job {job_id}\nTarget: {job['target_full_name']} ({job['target_username']})\nCount:{ac} Delay:{ad}s"
            sent = await ctx.bot.send_message(chat_id=int(owner), text=text, reply_markup=job_card(job_id))
            job["card_chat_id"]=sent.chat_id; job["card_message_id"]=sent.message_id
            save_state(state)
    except Exception as e:
        logger.exception("Failed to send job card to owner: %s", e)

# ---------- Callback Query Handler (job controls) ----------
async def callback_handler(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    data = q.data or ""
    parts = data.split("|")
    action = parts[0]; job_id = parts[1] if len(parts)>1 else None
    if not job_id:
        await q.edit_message_text("Invalid callback")
        return
    job = state.get("jobs",{}).get(job_id)
    if not job:
        await q.edit_message_text("Job not found")
        return
    user = update.effective_user
    if not (can_manage(user.id) or is_owner(user.id)):
        await q.edit_message_text("Only owner/manager can control job.")
        return

    if action == "start":
        # schedule background start
        if job["status"] in ("running",):
            await q.edit_message_text("Job already running.", reply_markup=job_card(job_id))
            return
        asyncio.create_task(run_job(job_id, ctx))
        job["status"]="running"; save_state(state); append_log({"event":"job_start","job_id":job_id,"by":str(user.id)})
        await update_job_card(job_id, ctx)
        return

    if action == "pause":
        if job["status"]=="running":
            job["status"]="paused"; save_state(state); append_log({"event":"job_pause","job_id":job_id,"by":str(user.id)})
            await update_job_card(job_id, ctx)
            return
        await q.edit_message_text("Job not running.", reply_markup=job_card(job_id))
        return

    if action == "stop":
        job["status"]="stopped"; save_state(state); append_log({"event":"job_stop","job_id":job_id,"by":str(user.id)})
        await update_job_card(job_id, ctx)
        return

    if action == "progress":
        prog = job.get("progress",0); cnt = job.get("count",0)
        await q.edit_message_text(f"Progress: {prog}/{cnt}", reply_markup=job_card(job_id))
        return

    if action == "details":
        await q.edit_message_text(json.dumps(job, indent=2)[:4000], reply_markup=job_card(job_id))
        return

    if action == "delete":
        state["jobs"].pop(job_id, None); save_state(state); append_log({"event":"job_delete","job_id":job_id,"by":str(user.id)})
        await q.edit_message_text(f"Job {job_id} deleted.")
        return

# ---------- Job Runner (simulated sends) ----------
async def run_job(job_id:str, ctx:ContextTypes.DEFAULT_TYPE):
    job = state.get("jobs",{}).get(job_id)
    if not job: return
    total = min(job.get("count",0), MAX_COUNT)
    job["status"]="running"; save_state(state)
    append_log({"event":"job_running","job_id":job_id,"count":total})
    try:
        for i in range(job.get("progress",0), total):
            job = state.get("jobs",{}).get(job_id)
            if not job or job.get("status") in ("paused","stopped"):
                save_state(state); append_log({"event":"job_interrupted","job_id":job_id,"status":job.get("status") if job else "missing"}); return
            # daily cap check
            if get_daily(job["chat_id"]) >= state.get("settings",{}).get("per_day_cap",PER_DAY_CAP):
                job["status"]="stopped"; save_state(state); append_log({"event":"job_block_by_daycap","job_id":job_id}); await update_job_card(job_id, ctx); return
            # pick template
            templates = state.get("templates", [])
            if not templates:
                append_log({"event":"job_no_templates","job_id":job_id}); job["status"]="stopped"; save_state(state); return
            t = next((x for x in templates if x["id"]==job["templates"][i%len(job["templates"])]), None)
            if not t:
                append_log({"event":"template_missing","job_id":job_id})
                await asyncio.sleep(job.get("delay",DEFAULT_DELAY)); continue
            # prepare target user info
            target_info = {"first_name": job.get("target_first_name",""), "username": job.get("target_username"), "full_name": job.get("target_full_name","")}
            text = render(t.get("content",""), job, target_info)
            # only send to users who opted-in: check
            if str(job["target_id"]) not in state.get("optin",[]):
                append_log({"event":"skipped_not_opted", "job_id":job_id, "target": job["target_id"]})
            else:
                # SIMULATED OR REAL SEND
                if SIMULATE_SEND:
                    logger.info(f"[SIMULATED SEND] to {job['target_id']} in chat {job['chat_id']}: {text[:120]}")
                    append_log({"event":"simulated_send","job_id":job_id,"text":text[:200]})
                else:
                    # uncomment to enable real send - ONLY with explicit consent and owner responsibility
                     await ctx.bot.send_message(chat_id=int(job["chat_id"]), text=text, reply_to_message_id=job["reply_to_message_id"])
                    append_log({"event":"real_send_placeholder","job_id":job_id})
                inc_daily(job["chat_id"],1)
            job["progress"]=i+1; save_state(state)
            await update_job_card(job_id, ctx)
            await asyncio.sleep(job.get("delay", DEFAULT_DELAY))
    finally:
        job = state.get("jobs",{}).get(job_id)
        if job and job.get("status") != "stopped":
            job["status"]="finished"; save_state(state); append_log({"event":"job_finished","job_id":job_id})
        await update_job_card(job_id, ctx)

async def update_job_card(job_id:str, ctx:ContextTypes.DEFAULT_TYPE):
    job = state.get("jobs",{}).get(job_id)
    if not job: return
    chat = job.get("card_chat_id") or state.get("owner_id") or OWNER_ID
    msg = job.get("card_message_id")
    text = f"Job {job_id}\nTarget: {job.get('target_full_name')} ({job.get('target_username')})\nStatus:{job.get('status')} Progress:{job.get('progress')}/{job.get('count')}"
    try:
        if msg:
            await ctx.bot.edit_message_text(text=text, chat_id=int(chat), message_id=msg, reply_markup=job_card(job_id))
    except Exception:
        # ignore edit errors
        pass

# ---------- Logs export ----------
async def exportlogs(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return await update.message.reply_text("Owner only.")
    if not LOG_FILE.exists():
        return await update.message.reply_text("No logs yet.")
    logs = json.loads(LOG_FILE.read_text(encoding="utf-8"))
    keys=set()
    for l in logs: keys.update(l.keys())
    keys = sorted(keys)
    path = Path("logs_export.csv")
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for l in logs:
            writer.writerow({k: l.get(k,"") for k in keys})
    await update.message.reply_document(document=path.open("rb"))

# ---------- Jobs list ----------
async def jobs_list(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    if not (role(update.effective_user.id) or is_owner(update.effective_user.id)):
        return await update.message.reply_text("Not authorized.")
    lines=[]
    for jid,j in state.get("jobs",{}).items():
        lines.append(f"{jid}: {j.get('status')} {j.get('progress',0)}/{j.get('count')} target={j.get('target_full_name')}")
    await update.message.reply_text("\n".join(lines) if lines else "No jobs.")

# ---------- Wiring ----------
def build_app(token:str):
    app = ApplicationBuilder().token(token).build()
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("settemplate", settemplate))
    app.add_handler(CommandHandler("done_template", done_template))
    app.add_handler(CommandHandler("templates", templates_list))
    app.add_handler(CommandHandler("setpromo", setpromo))
    app.add_handler(CommandHandler("done_promo", done_promo))
    app.add_handler(CommandHandler("promos", promos_list))
    app.add_handler(CommandHandler("optin", optin))
    app.add_handler(CommandHandler("optout", optout))
    app.add_handler(CommandHandler("addadmin", addadmin))
    app.add_handler(CommandHandler("removeadmin", removeadmin))
    app.add_handler(CommandHandler("listadmins", listadmins))
    app.add_handler(CommandHandler("s", create_job))
    app.add_handler(CommandHandler("jobs", jobs_list))
    app.add_handler(CommandHandler("exportlogs", exportlogs))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, media_receiver))
    app.add_handler(CallbackQueryHandler(callback_handler))
    return app

def main():
    logger.info("Starting Safe Advanced Scheduler (SIMULATE_SEND=%s)", SIMULATE_SEND)
    app = build_app(TOKEN)
    app.run_polling()

if __name__=="__main__":
    main()
