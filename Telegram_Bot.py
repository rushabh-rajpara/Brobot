from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters
from apscheduler.schedulers.background import BackgroundScheduler
import datetime
import asyncio
import json
import os

from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI")
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = int(os.getenv("CHAT_ID"))
client = MongoClient(MONGO_URI)
db = client["Brobot"]

goals_col = db["goals"]
schedule_col = db["schedule"]
pause_col = db["pause"]


# ✅ MongoDB Logic Replaces All File-Based Storage

def save_goal(goal):
    goals_col.replace_one({"type": "daily"}, {"type": "daily", "goal": goal}, upsert=True)

def get_goal():
    doc = goals_col.find_one({"type": "daily"})
    return doc["goal"] if doc else ""

def is_paused():
    doc = pause_col.find_one({"type": "pause"})
    return bool(doc)

def pause_bot():
    pause_col.replace_one({"type": "pause"}, {"type": "pause", "status": True}, upsert=True)

def resume_bot():
    pause_col.delete_one({"type": "pause"})

def load_schedule():
    schedule = {}
    for doc in schedule_col.find():
        schedule[doc["day"]] = doc["time"]
    return schedule

def save_schedule(data):
    schedule_col.delete_many({})
    for day, time in data.items():
        schedule_col.insert_one({"day": day, "time": time})


def is_working_now():
    schedule = load_schedule()
    now = datetime.datetime.now()
    day = now.strftime("%a").lower()[:3]

    if schedule.get(day) == "off":
        return False

    time_range = schedule.get(day)
    if time_range:
        try:
            start_str, end_str = time_range.split("-")
            start = datetime.datetime.strptime(start_str, "%H:%M").time()
            end = datetime.datetime.strptime(end_str, "%H:%M").time()
            now_time = now.time()
            return start <= now_time <= end
        except:
            return False

    return False









# List of rotating welcome messages
start_messages = [
    "Yo Rushabh! BRBot reporting for duty 💼",
    "Time to rise, grind, and code, boss! 💻",
    "I got your back today. Ready to crush it? 🚀",
    "Another day to dominate. Let’s gooo 🔥",
    "Morning, legend. Let’s get this bread. 🍞"
]

help_text = """
Here’s what I can do for you:
/start – Wake me up
/help – Show this list
/schedule – Set job hours
/goal – Set today's main goal
/status – Your current stats
/mood – how you're feeling today
/pause – Pause check-ins
/resume – Resume check-ins
/showschedule – View weekly job schedule
"""

import random

def load_lines(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f.readlines() if line.strip()]

morning_messages = load_lines('morning.txt')

checkin_options = [
    ["✅ Focused"],
    ["🕹️ Gaming / Watching"],
    ["📱 Scrolling / Wasting Time"],
    ["😴 Break / Nap"],
    ["✍️ Something else"]
]



import random

def load_lines(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f.readlines() if line.strip()]

# START COMMAND
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(random.choice(start_messages))

async def pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pause_bot()
    await update.message.reply_text("⛔ Bot check-ins paused. I’ll be chillin’ till you say `/resume`.")

async def resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    resume_bot()
    await update.message.reply_text("✅ Bot check-ins resumed. Let’s get back to work!")


# HELP COMMAND
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(help_text)

async def handle_checkin_response(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.lower().strip()

    # --- Handle goal setting if not already set
    if not get_goal():
        save_goal(text)
        await update.message.reply_text(f"🔒 Got it! Today’s goal locked in:\n\"{text}\"")
        return
    
    text = update.message.text.lower().strip()


    # --- Handle goal completion response
    if "yes" in text and get_goal():
        await update.message.reply_text(random.choice(load_lines('goal_done.txt')))
        save_goal("")  # Reset goal for tomorrow
        return

    elif "no" in text and get_goal():
        await update.message.reply_text(random.choice(load_lines('goal_missed.txt')))
        save_goal("")  # Reset goal for tomorrow
        return

    # --- Handle Check-in Responses
    if "focused" in text:
        await update.message.reply_text("Locked in. Keep crushing it 💪")

    elif "gaming" in text or "watching" in text or "scrolling" in text:
        await update.message.reply_text("Hmm... did you do your main tasks first? (Yes/No)")

    elif "break" in text:
        await update.message.reply_text("Short break? Cool. Don’t forget to bounce back ⏱️")

    elif "something" in text:
        await update.message.reply_text("Got it. BRBot is watching 👀")

    else:
        await update.message.reply_text("Hmm… noted. BRBot always remembers. 😎")



# UNKNOWN TEXTS
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Hey Rushabh, I didn't get that. Try /help if you're lost. 🧭")

async def send_morning_message(application):
    from telegram import Bot
    bot = Bot(token=BOT_TOKEN)
    message = random.choice(morning_messages)
    await bot.send_message(chat_id=CHAT_ID, text=f"🌞 Morning Rushabh!\n\n{message}")

async def ask_daily_goal(application):
    from telegram import Bot
    bot = Bot(token=BOT_TOKEN)
    await bot.send_message(
        chat_id=CHAT_ID,
        text="🎯 What's your main goal for today?\nJust reply with one sentence. No pressure, just purpose."
    )

async def set_goal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.args:
        goal = " ".join(context.args)
        save_goal(goal)
        await update.message.reply_text(f"🔒 New goal set:\n\"{goal}\"")
    else:
        await update.message.reply_text("Usage: /goal Your main goal for today")



async def send_checkin(application):
    if is_paused() or is_working_now():
        return
    
    from telegram import Bot
    bot = Bot(token=BOT_TOKEN)

    markup = ReplyKeyboardMarkup(checkin_options, one_time_keyboard=True, resize_keyboard=True)
    await bot.send_message(
        chat_id=CHAT_ID,
        text="🔄 Check-in time!\nWhat are you up to right now?",
        reply_markup=markup
    )

async def ask_goal_completion(application):
    from telegram import Bot
    bot = Bot(token=BOT_TOKEN)
    goal = get_goal()

    if goal:
        await bot.send_message(
            chat_id=CHAT_ID,
            text=f"{random.choice(load_lines('night.txt'))}{goal}\"\n(Yes / No)"
        )
    else:
        await bot.send_message(
            chat_id=CHAT_ID,
            text="🌙 Did you have a goal today? 🤔 I couldn’t find one logged."
        )
    
async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    goal = get_goal() or "No goal set yet"
    paused = "✅ Active" if not is_paused() else "⛔ Paused"

    msg = f"""📊 Your Current Status:
🎯 Goal: {goal}
🕹️ Bot Mode: {paused}
"""
    await update.message.reply_text(msg)




async def set_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /schedule <day> <start-end> or 'off'\nExample: /schedule mon 10:00-18:00")
        return

    day = context.args[0].lower()[:3]
    time_range = context.args[1].lower()

    schedule = load_schedule()

    if time_range == "off":
        schedule[day] = "off"
        save_schedule(schedule)
        await update.message.reply_text(f"📅 Schedule updated: {day.title()} is now a day off.")
    else:
        try:
            start, end = time_range.split("-")
            # Optional: Validate format here
            schedule[day] = f"{start}-{end}"
            save_schedule(schedule)
            await update.message.reply_text(f"📅 Schedule updated: {day.title()} → {start} to {end}")
        except ValueError:
            await update.message.reply_text("Time format should be like 10:00-18:00")

async def show_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    schedule = load_schedule()
    msg = "📅 Your Weekly Schedule:\n"
    for day in ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]:
        entry = schedule.get(day, "Not set")
        msg += f"{day.title()}: {entry}\n"
    await update.message.reply_text(msg)


async def weekly_report(application):
    if is_paused():
        return

    from telegram import Bot
    bot = Bot(token=BOT_TOKEN)

    goals_completed = 0
    lazy_days = 0

    today = datetime.date.today()

    for i in range(7):
        day = today - datetime.timedelta(days=i)
        day_str = day.isoformat()

        

        # Goal check (we assume if mood was logged, day was active)
        goal_doc = goals_col.find_one({"date": day_str})
        if goal_doc and goal_doc.get("done") is True:
            goals_completed += 1

    

    report = f"""📊 Weekly Report – {today.strftime('%B %d, %Y')}

✅ Goals completed: {goals_completed}/7  
😐 Lazy/no-goal days: {lazy_days}  


Let’s aim even higher next week, king 👑
"""

    await bot.send_message(chat_id=CHAT_ID, text=report)




# MAIN FUNCTION
if __name__ == '__main__':
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_checkin_response))
    app.add_handler(CommandHandler("pause", pause))
    app.add_handler(CommandHandler("resume", resume))
    app.add_handler(CommandHandler("goal", set_goal))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("schedule", set_schedule))
    app.add_handler(CommandHandler("showschedule", show_schedule))






    # Set up daily scheduler
    scheduler = BackgroundScheduler()
    scheduler.add_job(lambda: asyncio.run(send_morning_message(app)), 'cron', hour=9, minute=00)
    # 9:05 AM - ask for goal
    scheduler.add_job(lambda: asyncio.run(ask_daily_goal(app)), 'cron', hour=9, minute=5)



    # 10:30 PM - ask if goal was completed
    scheduler.add_job(lambda: asyncio.run(ask_goal_completion(app)), 'cron', hour=22, minute=30)
    scheduler.start()

    scheduler.add_job(
    lambda: asyncio.run(weekly_report(app)),
    'cron',
    day_of_week='sun',
    hour=9,
    minute=0
    )

for hour in [11, 13, 15, 17, 19]:  # Adjust these times if needed
    scheduler.add_job(
        lambda: asyncio.run(send_checkin(app)),
        'cron',
        hour=hour,
        minute=0
    )


    
    print("BRBot is running with scheduled morning message... 🌞")
    app.run_polling()
