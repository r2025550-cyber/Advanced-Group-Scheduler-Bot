"""
Combo Bot = Advanced Scheduler + Promotions
⚠️ Educational use only. Telegram flood control लागू है।
"""

import os, json, uuid, logging, asyncio, sqlite3, csv, time
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes

# ---------------- CONFIG ----------------
DB_FILE = "bot_data.db"
SIMULATE_SEND = False       # False = real send
MAX_COUNT = 200
PER_DAY_CAP = 1000
DEFAULT_COUNT = 3
DEFAULT_DELAY = 2.0
PER_USER_COOLDOWN_SECONDS = 60

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
OWNER_ID = os.getenv("OWNER_ID")
if not TELEGRAM_TOKEN:
    raise SystemExit("Set TELEGRAM_TOKEN env var")

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("combo_bot")

# ---------------- DB ----------------
def db_connect():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn
DB = db_connect()
def init_db():
    cur = DB.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS templates (id TEXT PRIMARY KEY, content TEXT, ts TEXT);
    CREATE TABLE IF NOT EXISTS promos (id TEXT PRIMARY KEY, content TEXT, ts TEXT);
    CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, chat_id INTEGER, reply_msg_id INTEGER,
        created_by TEXT, target_id TEXT, target_username TEXT, target_name TEXT,
        templates_json TEXT, count INTEGER, delay REAL, status TEXT, progress INTEGER,
        card_chat_id INTEGER, card_msg_id INTEGER, ts TEXT);
    CREATE TABLE IF NOT EXISTS campaigns (id TEXT PRIMARY KEY, promo_id TEXT, status TEXT, progress INTEGER, ts TEXT);
    CREATE TABLE IF NOT EXISTS logs (id INTEGER PRIMARY KEY AUTOINCREMENT, event TEXT, data TEXT, ts TEXT);
    CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
    """)
    DB.commit()
init_db()

def db_set_meta(k,v): DB.execute("INSERT OR REPLACE INTO meta(k,v) VALUES (?,?)",(k,str(v))); DB.commit()
def db_get_meta(k,d=None): r=DB.execute("SELECT v FROM meta WHERE k=?",(k,)).fetchone(); return r["v"] if r else d
def append_log(e,d): DB.execute("INSERT INTO logs(event,data,ts) VALUES (?,?,?)",(e,json.dumps(d),datetime.utcnow().isoformat())); DB.commit()

# ---------------- Helpers ----------------
def is_owner(uid): return str(uid)==str(OWNER_ID)
def now_iso(): return datetime.utcnow().isoformat()

def save_template(content:str):
    tid=uuid.uuid4().hex[:8]; DB.execute("INSERT INTO templates VALUES (?,?,?)",(tid,content,now_iso())); DB.commit()
    append_log("template_saved",{"id":tid}); return tid
def save_promo(content:str):
    pid=uuid.uuid4().hex[:8]; DB.execute("INSERT INTO promos VALUES (?,?,?)",(pid,content,now_iso())); DB.commit()
    append_log("promo_saved",{"id":pid}); return pid
def list_templates(): return [dict(r) for r in DB.execute("SELECT * FROM templates")]
def list_promos(): return [dict(r) for r in DB.execute("SELECT * FROM promos")]

def create_job_db(j): DB.execute("""INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
    (j["job_id"],j["chat_id"],j["reply_msg_id"],j["created_by"],j["target_id"],j["target_username"],
     j["target_name"],json.dumps(j["templates"]),j["count"],j["delay"],j["status"],j["progress"],
     j["card_chat_id"],j["card_msg_id"],now_iso())); DB.commit()
def update_job_db(jid,u): DB.execute(f"UPDATE jobs SET {','.join([k+'=?' for k in u])} WHERE id=?",
    list(u.values())+[jid]); DB.commit()
def get_job(jid): r=DB.execute("SELECT * FROM jobs WHERE id=?",(jid,)).fetchone(); return dict(r) if r else None
def list_jobs(): return [dict(r) for r in DB.execute("SELECT * FROM jobs")]

def inc_daily(chat_id,n=1):
    k=f"daily|{chat_id}|{datetime.utcnow().date().isoformat()}"; v=int(db_get_meta(k,"0"))+n; db_set_meta(k,v); return v
def get_daily(chat_id): return int(db_get_meta(f"daily|{chat_id}|{datetime.utcnow().date().isoformat()}","0"))

# ---------------- UI ----------------
def job_card_kb(jid): return InlineKeyboardMarkup([
    [InlineKeyboardButton("▶ Start",callback_data=f"job_start|{jid}"),
     InlineKeyboardButton("⏸ Pause",callback_data=f"job_pause|{jid}"),
     InlineKeyboardButton("⏹ Stop",callback_data=f"job_stop|{jid}")],
    [InlineKeyboardButton("📋 Details",callback_data=f"job_details|{jid}"),
     InlineKeyboardButton("📊 Progress",callback_data=f"job_progress|{jid}"),
     InlineKeyboardButton("🗑 Delete",callback_data=f"job_delete|{jid}")]
])
def promo_card_kb(pid): return InlineKeyboardMarkup([
    [InlineKeyboardButton("▶ Start Campaign",callback_data=f"promo_start|{pid}"),
     InlineKeyboardButton("⏸ Pause",callback_data=f"promo_pause|{pid}")],
    [InlineKeyboardButton("⏹ Stop",callback_data=f"promo_stop|{pid}"),
     InlineKeyboardButton("📊 Analytics",callback_data=f"promo_stats|{pid}")]
])

# ---------------- Commands ----------------
async def start_cmd(u,c): await u.message.reply_text("Combo Bot ✅\nUse /help")
async def help_cmd(u,c):
    await u.message.reply_text(
        "📌 Spam Scheduler:\n"
        "/settemplate, /clear_template, /templates\n"
        "Reply + /s count=.. delay=..\n"
        "/setcount, /setdelay\n\n"
        "📌 Promotions:\n"
        "/setpromo, /promos\n\n"
        "📌 Admin:\n"
        "/addadmin, /addmanager, /removeadmin\n"
        "/jobs, /exportlogs"
    )

# Templates
async def settemplate(u,c):
    if not is_owner(u.effective_user.id): return await u.message.reply_text("Owner only")
    c.user_data["rec_template"]=True; await u.message.reply_text("Send template text, finish with /done_template")
async def done_template(u,c):
    if c.user_data.pop("rec_template",False): await u.message.reply_text("Templates saved.")
async def clear_template(u,c):
    if not is_owner(u.effective_user.id): return await u.message.reply_text("Owner only")
    DB.execute("DELETE FROM templates"); DB.commit(); await u.message.reply_text("All templates cleared.")
async def templates_list(u,c):
    rows=list_templates(); 
    if not rows: return await u.message.reply_text("No templates")
    await u.message.reply_text("\n".join([f"{r['id']}: {r['content'][:50]}" for r in rows]))
# Promotions
async def setpromo(u,c):
    if not is_owner(u.effective_user.id): return await u.message.reply_text("Owner only")
    c.user_data["rec_promo"]=True; await u.message.reply_text("Send promo text, finish with /done_promo")
async def done_promo(u,c):
    if c.user_data.pop("rec_promo",False): await u.message.reply_text("Promos saved.")
async def promos_list(u,c):
    rows=list_promos(); 
    if not rows: return await u.message.reply_text("No promos")
    msg="\n".join([f"{r['id']}: {r['content'][:40]}" for r in rows])
    await u.message.reply_text(msg)
    for r in rows:
        await u.message.reply_text(r['content'], reply_markup=promo_card_kb(r['id']))

# Media receiver
async def media_receiver(u,c):
    if c.user_data.get("rec_template") and u.message.text:
        tid=save_template(u.message.text); await u.message.reply_text(f"Saved template {tid}")
    if c.user_data.get("rec_promo") and u.message.text:
        pid=save_promo(u.message.text); await u.message.reply_text(f"Saved promo {pid}")

# Scheduler Job
async def create_job(u,c):
    if not is_owner(u.effective_user.id): return await u.message.reply_text("Owner only")
    if not u.message.reply_to_message: return await u.message.reply_text("Reply + /s")
    count=DEFAULT_COUNT; delay=DEFAULT_DELAY
    for p in (" ".join(c.args)).split():
        if "=" in p: k,v=p.split("=",1); 
        if k=="count": 
            try: count=min(int(v),MAX_COUNT)
            except: pass
        if k=="delay":
            try: delay=float(v)
            except: pass
    templates=list_templates()
    if not templates: return await u.message.reply_text("No templates. Use /settemplate")
    r=u.message.reply_to_message; t=r.from_user
    jid=uuid.uuid4().hex[:8]
    job={"job_id":jid,"chat_id":u.effective_chat.id,"reply_msg_id":r.message_id,
         "created_by":str(u.effective_user.id),"target_id":str(t.id),"target_username":t.username or "",
         "target_name":t.full_name if hasattr(t,"full_name") else (t.first_name or ""),
         "templates":[x["id"] for x in templates],"count":count,"delay":delay,
         "status":"queued","progress":0,"card_chat_id":None,"card_msg_id":None}
    create_job_db(job); append_log("job_created",{"job_id":jid})
    await u.message.reply_text("✅ Job created, check owner DM")
    if OWNER_ID:
        sent=await c.bot.send_message(int(OWNER_ID),f"Job {jid} Target:{job['target_name']} ({job['target_username']}) Count={count} Delay={delay}",
                                      reply_markup=job_card_kb(jid))
        update_job_db(jid,{"card_chat_id":sent.chat_id,"card_msg_id":sent.message_id})

# Job Runner
async def run_job_runner(jid,c):
    j=get_job(jid); 
    if not j: return
    total=min(int(j["count"]),MAX_COUNT)
    update_job_db(jid,{"status":"running"}); append_log("job_running",{"job_id":jid})
    try:
        for i in range(int(j["progress"]),total):
            j=get_job(jid)
            if not j or j["status"] in ("paused","stopped"): return
            if get_daily(j["chat_id"])>=PER_DAY_CAP:
                update_job_db(jid,{"status":"stopped"}); append_log("blocked_daycap",{"job_id":jid}); return
            templ_ids=json.loads(j["templates"]) if isinstance(j["templates"],str) else j["templates"]
            t_id=templ_ids[i%len(templ_ids)]; row=DB.execute("SELECT content FROM templates WHERE id=?",(t_id,)).fetchone()
            if not row: continue
            text=row["content"].replace("{target_name}",j["target_name"]).replace("{username}",("@"+j["target_username"]) if j["target_username"] else j["target_name"])
            # cooldown
            key=f"lastsend|{j['target_id']}"; last=float(db_get_meta(key,"0")); now=time.time()
            if now-last<PER_USER_COOLDOWN_SECONDS: 0
            if SIMULATE_SEND:
                logger.info(f"[SIMULATED] {text}")
            else:
                await c.bot.send_message(int(j["chat_id"]),text,reply_to_message_id=int(j["reply_msg_id"]))
            inc_daily(j["chat_id"],1); db_set_meta(key,str(now))
            update_job_db(jid,{"progress":i+1})
            await asyncio.sleep(j["delay"])
    finally:
        update_job_db(jid,{"status":"finished"}); append_log("job_finished",{"job_id":jid})

# Callback Handler
async def callback_handler(u,c):
    q=u.callback_query; await q.answer(); d=q.data.split("|"); a=d[0]; pid_or_jid=d[1]
    if not is_owner(u.effective_user.id): return await q.edit_message_text("Owner only")
    if a.startswith("job"):
        j=get_job(pid_or_jid); 
        if not j: return await q.edit_message_text("Job not found")
        if a=="job_start": asyncio.create_task(run_job_runner(pid_or_jid,c)); update_job_db(pid_or_jid,{"status":"running"}); return
        if a=="job_pause": update_job_db(pid_or_jid,{"status":"paused"}); return
        if a=="job_stop": update_job_db(pid_or_jid,{"status":"stopped"}); return
        if a=="job_progress": await q.edit_message_text(f"{j['progress']}/{j['count']}",reply_markup=job_card_kb(pid_or_jid)); return
        if a=="job_details": await q.edit_message_text(json.dumps(j,indent=2)[:4000],reply_markup=job_card_kb(pid_or_jid)); return
        if a=="job_delete": DB.execute("DELETE FROM jobs WHERE id=?",(pid_or_jid,)); DB.commit(); await q.edit_message_text("Deleted")
    if a.startswith("promo"):
        if a=="promo_start": append_log("promo_start",{"id":pid_or_jid}); await q.edit_message_text("Promo campaign started",reply_markup=promo_card_kb(pid_or_jid))
        if a=="promo_pause": append_log("promo_pause",{"id":pid_or_jid}); await q.edit_message_text("Promo paused",reply_markup=promo_card_kb(pid_or_jid))
        if a=="promo_stop": append_log("promo_stop",{"id":pid_or_jid}); await q.edit_message_text("Promo stopped")
        if a=="promo_stats": await q.edit_message_text("📊 Promo Analytics (dummy)",reply_markup=promo_card_kb(pid_or_jid))

# Logs Export
async def exportlogs(u,c):
    if not is_owner(u.effective_user.id): return await u.message.reply_text("Owner only")
    rows=DB.execute("SELECT * FROM logs").fetchall()
    if not rows: return await u.message.reply_text("No logs")
    f="logs.csv"; w=open(f,"w",newline="",encoding="utf-8"); cw=csv.writer(w); cw.writerow(["id","event","data","ts"])
    for r in rows: cw.writerow([r["id"],r["event"],r["data"],r["ts"]]); w.close()
    await u.message.reply_document(open(f,"rb"))

async def jobs_list(u,c):
    if not is_owner(u.effective_user.id): return await u.message.reply_text("Owner only")
    rows=list_jobs(); 
    if not rows: return await u.message.reply_text("No jobs")
    await u.message.reply_text("\n".join([f"{r['id']}: {r['status']} {r['progress']}/{r['count']}" for r in rows]))

# ---------------- Wiring ----------------
def build_app(tk):
    app=ApplicationBuilder().token(tk).build()
    app.add_handler(CommandHandler("start",start_cmd))
    app.add_handler(CommandHandler("help",help_cmd))
    app.add_handler(CommandHandler("settemplate",settemplate))
    app.add_handler(CommandHandler("done_template",done_template))
    app.add_handler(CommandHandler("clear_template",clear_template))
    app.add_handler(CommandHandler("templates",templates_list))
    app.add_handler(CommandHandler("setpromo",setpromo))
    app.add_handler(CommandHandler("done_promo",done_promo))
    app.add_handler(CommandHandler("promos",promos_list))
    app.add_handler(CommandHandler("s",create_job))
    app.add_handler(CommandHandler("jobs",jobs_list))
    app.add_handler(CommandHandler("exportlogs",exportlogs))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND,media_receiver))
    app.add_handler(CallbackQueryHandler(callback_handler))
    return app

def main(): logger.info("Starting Combo Bot..."); app=build_app(TELEGRAM_TOKEN); app.run_polling()
if __name__=="__main__": main()

