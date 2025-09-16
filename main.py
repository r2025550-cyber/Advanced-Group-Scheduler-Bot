"""
Pro Advanced Group Scheduler Bot with Attractive Buttons
Author: Vineet + ChatGPT
"""
import os, json, logging, asyncio, uuid, csv
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
MAX_COUNT_HARD = 200
PER_DAY_GROUP_CAP = 1000
DEFAULT_COUNT = 3
DEFAULT_DELAY = 2.0
SAFE_MODE_DEFAULT = True
# ----------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("advanced_scheduler")

# -------------- Persistence --------------
def load_state() -> Dict[str, Any]:
    if STATE_FILE.exists():
        try: return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except: logger.exception("Failed to load state.")
    return {"owner_id": None, "admins": {}, "templates": [], "jobs": {}, 
            "settings": {"max_count": MAX_COUNT_HARD,
                         "per_day_group_cap": PER_DAY_GROUP_CAP,
                         "safe_mode": SAFE_MODE_DEFAULT},
            "daily_counts": {}}

def save_state(s: Dict[str, Any]): STATE_FILE.write_text(json.dumps(s, indent=2))
def append_log(e: Dict[str, Any]):
    logs=[]
    if LOG_FILE.exists():
        try: logs=json.loads(LOG_FILE.read_text())
        except: pass
    e["ts"]=datetime.utcnow().isoformat(); logs.append(e)
    LOG_FILE.write_text(json.dumps(logs, indent=2))

state = load_state()
running_tasks: Dict[str, asyncio.Task] = {}

# RBAC helpers
def is_owner(uid): return str(uid)==str(os.getenv("OWNER_ID"))
def get_role(uid): return "owner" if is_owner(uid) else state.get("admins",{}).get(str(uid))
def can_manage(uid): return get_role(uid) in ("owner","manager")
def can_edit(uid): return get_role(uid) in ("owner","manager","editor")

# daily counters
def today_key(cid): return f"{datetime.utcnow().date()}|{cid}"
def inc_daily(cid,a=1): k=today_key(cid); state["daily_counts"][k]=state["daily_counts"].get(k,0)+a; save_state(state); return state["daily_counts"][k]
def get_daily(cid): return state.get("daily_counts",{}).get(today_key(cid),0)

# placeholder renderer
def render(text,user):
    now=datetime.utcnow()
    repl={"{username}":getattr(user,"username","") or "",
          "{first_name}":getattr(user,"first_name","") or "",
          "{last_name}":getattr(user,"last_name","") or "",
          "{date}":now.date().isoformat(),
          "{time}":now.time().strftime("%H:%M:%S")}
    for k,v in repl.items(): text=text.replace(k,v)
    return text

# parse kv args
def parse_kv(argstr): return {p.split("=",1)[0]:p.split("=",1)[1] for p in argstr.split() if "=" in p}
def parse_time(s): 
    try: h,m=s.split(":"); return dtime(int(h),int(m))
    except: return None

# -------- Job Card UI --------
def job_card_markup(jid):
    kb=[
        [InlineKeyboardButton("▶ Start",callback_data=f"start|{jid}"),
         InlineKeyboardButton("⏸ Pause",callback_data=f"pause|{jid}"),
         InlineKeyboardButton("⏹ Stop",callback_data=f"stop|{jid}")],
        [InlineKeyboardButton("🔄 Clone",callback_data=f"clone|{jid}"),
         InlineKeyboardButton("✏️ Edit",callback_data=f"edit|{jid}"),
         InlineKeyboardButton("📋 Details",callback_data=f"details|{jid}")],
        [InlineKeyboardButton("📊 Progress",callback_data=f"progress|{jid}"),
         InlineKeyboardButton("🗑 Delete",callback_data=f"delete|{jid}"),
         InlineKeyboardButton("📑 Logs",callback_data=f"logs|{jid}")]
    ]
    return InlineKeyboardMarkup(kb)

# ---------------- Commands ----------------
async def start_cmd(u,c): await u.message.reply_text("✅ Bot running. Use /help")
async def help_cmd(u,c): await u.message.reply_text("Commands:\n/settemplate /done_template /templates\n/s count=5 delay=2\n/jobs /exportlogs")

async def settemplate(u,c):
    if not can_edit(u.effective_user.id): return await u.message.reply_text("Permission denied.")
    c.user_data["rec"]=True; await u.message.reply_text("Recording template. Send text/photo then /done_template")

async def done_template(u,c):
    if c.user_data.pop("rec",False): save_state(state); await u.message.reply_text("Template saved.")
    else: await u.message.reply_text("Not recording.")

async def media_recv(u,c):
    if c.user_data.get("rec"):
        if u.message.photo: state["templates"].append({"type":"photo","file_id":u.message.photo[-1].file_id,"content":u.message.caption or ""})
        elif u.message.text: state["templates"].append({"type":"text","content":u.message.text})
        save_state(state); await u.message.reply_text("Template stored."); return

async def templates_list(u,c):
    if not state["templates"]: return await u.message.reply_text("No templates.")
    await u.message.reply_text("\n".join([f"{i+1}. {t['type']} - {t.get('content','')[:30]}" for i,t in enumerate(state["templates"])]))

# Admin mgmt
async def addadmin(u,c):
    if not is_owner(u.effective_user.id): return await u.message.reply_text("Owner only.")
    if len(c.args)<2: return await u.message.reply_text("Usage: /addadmin <id> <role>")
    state["admins"][c.args[0]]=c.args[1]; save_state(state); await u.message.reply_text("Admin added.")
async def listadmins(u,c): await u.message.reply_text(str(state["admins"]))

# ---------------- Jobs ----------------
async def create_job(u,c):
    if not (can_manage(u.effective_user.id) or is_owner(u.effective_user.id)):
        return await u.message.reply_text("Not allowed.")
    if not u.message.reply_to_message: return await u.message.reply_text("Reply to a user and send /s")
    if not state["templates"]: return await u.message.reply_text("No templates saved.")
    opts=parse_kv(" ".join(c.args)) if c.args else {}
    count=min(int(opts.get("count",DEFAULT_COUNT)), state["settings"]["max_count"])
    delay=float(opts.get("delay",DEFAULT_DELAY))
    jid=uuid.uuid4().hex[:6]
    job={"job_id":jid,"chat_id":u.effective_chat.id,
         "reply_to":u.message.reply_to_message.message_id,
         "target_name":u.message.reply_to_message.from_user.full_name,
         "templates":state["templates"],"count":count,"delay":delay,
         "status":"queued","progress":0}
    state["jobs"][jid]=job; save_state(state)
    sent=await u.message.reply_text(f"🎯 Job {jid} created",reply_markup=job_card_markup(jid))
    job["card_chat_id"]=sent.chat_id; job["card_message_id"]=sent.message_id; save_state(state)

async def callback_handler(u,c):
    q=u.callback_query; await q.answer()
    act,jid=q.data.split("|"); job=state["jobs"].get(jid)
    if not job: return await q.edit_message_text("Job not found.")
    if act=="start": asyncio.create_task(run_job(jid,c))
    elif act=="pause": job["status"]="paused"; save_state(state)
    elif act=="stop": job["status"]="stopped"; save_state(state)
    elif act=="progress": await q.edit_message_text(f"📊 {job['progress']}/{job['count']}",reply_markup=job_card_markup(jid))
    elif act=="delete": state["jobs"].pop(jid,None); save_state(state); await q.edit_message_text(f"🗑 Job {jid} deleted.")
    elif act=="logs":
        logs=json.loads(LOG_FILE.read_text()) if LOG_FILE.exists() else []
        jlogs=[l for l in logs if l.get("job_id")==jid]
        txt="\n".join([f"{l['ts']} - {l['event']}" for l in jlogs[-5:]]) or "No logs"
        await q.edit_message_text(f"📑 Logs for {jid}:\n{txt}")
    else: await q.edit_message_text("Action not implemented.")

async def run_job(jid,c):
    job=state["jobs"].get(jid); 
    if not job: return
    job["status"]="running"; save_state(state)
    for i in range(job["progress"],job["count"]):
        if job["status"]!="running": break
        tpl=job["templates"][i%len(job["templates"])]
        try:
            if tpl["type"]=="text":
                await c.bot.send_message(chat_id=job["chat_id"], text=tpl["content"], reply_to_message_id=job["reply_to"])
            elif tpl["type"]=="photo":
                await c.bot.send_photo(chat_id=job["chat_id"], photo=tpl["file_id"], caption=tpl.get("content",""), reply_to_message_id=job["reply_to"])
            inc_daily(job["chat_id"]); job["progress"]=i+1; save_state(state)
        except Exception as e: logger.error(e)
        await asyncio.sleep(job["delay"])
    job["status"]="finished"; save_state(state)

# ---------------- Logs Export ----------------
async def exportlogs(u,c):
    if not is_owner(u.effective_user.id): return await u.message.reply_text("Owner only")
    if not LOG_FILE.exists(): return await u.message.reply_text("No logs")
    logs=json.loads(LOG_FILE.read_text())
    keys=set(); [keys.update(l.keys()) for l in logs]
    with open("logs.csv","w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=list(keys)); w.writeheader(); [w.writerow(l) for l in logs]
    await u.message.reply_document("logs.csv")

# ---------------- Wiring ----------------
def build_app(tok):
    a=ApplicationBuilder().token(tok).build()
    a.add_handler(CommandHandler("start",start_cmd))
    a.add_handler(CommandHandler("help",help_cmd))
    a.add_handler(CommandHandler("settemplate",settemplate))
    a.add_handler(CommandHandler("done_template",done_template))
    a.add_handler(CommandHandler("templates",templates_list))
    a.add_handler(CommandHandler("addadmin",addadmin))
    a.add_handler(CommandHandler("listadmins",listadmins))
    a.add_handler(CommandHandler("s",create_job))
    a.add_handler(CommandHandler("exportlogs",exportlogs))
    a.add_handler(MessageHandler(filters.PHOTO | (filters.TEXT & ~filters.COMMAND),media_recv))
    a.add_handler(CallbackQueryHandler(callback_handler))
    return a

def main():
    tok=os.getenv("TELEGRAM_TOKEN"); 
    if not tok: return logger.error("Token missing")
    logger.info("Starting bot...")
    build_app(tok).run_polling()

if __name__=="__main__": main()
