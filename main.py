import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes
)
import asyncio

# ----------------- CONFIG -----------------
BOT_TOKEN = "8428714734:AAFQ5TOPgxXdjsS_KxOLogGY7kowLGXxHSY"  # 🔴 यहां अपना bot token डालो
OWNER_ID = 7081155872           # 🔴 यहां अपना Telegram ID डालो
DEFAULT_COUNT = 200
DEFAULT_DELAY = 2
DAILY_LIMIT = 1000

# ----------------- LOGGING -----------------
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("advanced_scheduler")

# ----------------- DATA -----------------
jobs = {}
user_limits = {}
admins = set()

# ----------------- BUTTONS -----------------
def main_menu():
    kb = [
        [InlineKeyboardButton("📢 Promotion", callback_data="promotion"),
         InlineKeyboardButton("🎯 Targeting", callback_data="targeting")],
        [InlineKeyboardButton("📊 Analytics", callback_data="analytics"),
         InlineKeyboardButton("⚙️ Settings", callback_data="settings")],
        [InlineKeyboardButton("📑 Logs", callback_data="logs"),
         InlineKeyboardButton("👑 Admin Panel", callback_data="admin_panel")]
    ]
    return InlineKeyboardMarkup(kb)


def promotion_menu():
    kb = [
        [InlineKeyboardButton("➕ New Promotion", callback_data="promo_new"),
         InlineKeyboardButton("✏️ Edit Promotion", callback_data="promo_edit")],
        [InlineKeyboardButton("▶️ Start", callback_data="promo_start"),
         InlineKeyboardButton("⏸ Pause", callback_data="promo_pause"),
         InlineKeyboardButton("⏹ Stop", callback_data="promo_stop")],
        [InlineKeyboardButton("🔄 Clone", callback_data="promo_clone"),
         InlineKeyboardButton("🗑 Delete", callback_data="promo_delete")],
        [InlineKeyboardButton("⬅️ Back", callback_data="back_main")]
    ]
    return InlineKeyboardMarkup(kb)


def targeting_menu():
    kb = [
        [InlineKeyboardButton("👤 Reply Target", callback_data="tgt_reply"),
         InlineKeyboardButton("📜 Upload List", callback_data="tgt_list")],
        [InlineKeyboardButton("🌍 Location Filter", callback_data="tgt_location"),
         InlineKeyboardButton("🔁 Rotation Mode", callback_data="tgt_rotation")],
        [InlineKeyboardButton("⬅️ Back", callback_data="back_main")]
    ]
    return InlineKeyboardMarkup(kb)


def analytics_menu():
    kb = [
        [InlineKeyboardButton("📊 Daily Report", callback_data="rep_daily"),
         InlineKeyboardButton("📈 Charts", callback_data="rep_chart")],
        [InlineKeyboardButton("📥 Export CSV", callback_data="rep_csv"),
         InlineKeyboardButton("📑 Logs", callback_data="rep_logs")],
        [InlineKeyboardButton("⬅️ Back", callback_data="back_main")]
    ]
    return InlineKeyboardMarkup(kb)


def settings_menu():
    kb = [
        [InlineKeyboardButton("🔢 Set Count", callback_data="set_count"),
         InlineKeyboardButton("⏱ Set Delay", callback_data="set_delay")],
        [InlineKeyboardButton("📆 Schedule", callback_data="set_schedule"),
         InlineKeyboardButton("🚫 Daily Limit", callback_data="set_limit")],
        [InlineKeyboardButton("⬅️ Back", callback_data="back_main")]
    ]
    return InlineKeyboardMarkup(kb)


def admin_panel():
    kb = [
        [InlineKeyboardButton("➕ Add Admin", callback_data="add_admin"),
         InlineKeyboardButton("➖ Remove Admin", callback_data="remove_admin")],
        [InlineKeyboardButton("📋 List Admins", callback_data="list_admins")],
        [InlineKeyboardButton("⬅️ Back", callback_data="back_main")]
    ]
    return InlineKeyboardMarkup(kb)

# ----------------- COMMANDS -----------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 Pro Scheduler Bot Activated!\nChoose an option:",
        reply_markup=main_menu()
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "📌 Commands:\n"
        "/s (reply to user) → Start job for that user\n"
        "/setcount <n> → Set default message count\n"
        "/setdelay <s> → Set delay in seconds\n"
        "/addadmin <id> → Add admin\n"
        "/removeadmin <id> → Remove admin\n"
        "/listadmins → Show admins\n"
    )
    await update.message.reply_text(text)

# ----------------- CALLBACK HANDLER -----------------
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "promotion":
        await query.edit_message_text("📢 Promotion Menu:", reply_markup=promotion_menu())
    elif query.data == "targeting":
        await query.edit_message_text("🎯 Targeting Menu:", reply_markup=targeting_menu())
    elif query.data == "analytics":
        await query.edit_message_text("📊 Analytics Menu:", reply_markup=analytics_menu())
    elif query.data == "settings":
        await query.edit_message_text("⚙️ Settings Menu:", reply_markup=settings_menu())
    elif query.data == "logs":
        await query.edit_message_text("📑 Logs: (Work in progress)")
    elif query.data == "admin_panel":
        if update.effective_user.id == OWNER_ID:
            await query.edit_message_text("👑 Admin Panel:", reply_markup=admin_panel())
        else:
            await query.edit_message_text("❌ Only owner can access Admin Panel.")
    elif query.data == "back_main":
        await query.edit_message_text("🤖 Main Menu:", reply_markup=main_menu())

# ----------------- GROUP SMS FEATURE -----------------
async def s_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message:
        await update.message.reply_text("❌ Reply to a user's message with /s")
        return

    target = update.message.reply_to_message.from_user
    username = f"@{target.username}" if target.username else target.first_name
    count = DEFAULT_COUNT
    delay = DEFAULT_DELAY

    await update.message.reply_text(
        f"✅ Job started for {username} ({target.first_name})\n"
        f"Count: {count}, Delay: {delay}s"
    )

    for i in range(count):
        await asyncio.sleep(delay)
        try:
            await update.message.reply_to_message.reply_text(f"{username} I love you ❤️")
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            break

# ----------------- MAIN -----------------
def main():
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("s", s_cmd))
    app.add_handler(CallbackQueryHandler(button_handler))

    logger.info("Starting Advanced Scheduler Bot...")
    app.run_polling()

if __name__ == "__main__":
    main()
