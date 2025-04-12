from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters
from apscheduler.schedulers.background import BackgroundScheduler
import datetime
import asyncio
import json
import os
from pytz import timezone

import cohere
cohere_client = cohere.Client(os.getenv("COHERE_API_KEY"))  # Load from environment
api_call_count = {"cohere": 0}

def get_cohere_reply(prompt):
    try:
        global api_call_count
        api_call_count["cohere"] += 1
        response = cohere_client.chat(
            message=prompt,
            model="command-r-plus",
            temperature=0.7
        )
        return response.text
    except Exception as e:
        return "⚠️ Couldn't reach Cohere: " + str(e)



from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI")
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = int(os.getenv("CHAT_ID"))
client = MongoClient(MONGO_URI)
db = client["Brobot"]

toronto = timezone("America/Toronto")


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
/pause – Pause check-ins
/resume – Resume check-ins
/showschedule – View weekly job schedule
/streak – Show your goal streak and progress
/apicount – Show Cohere API usage
/history – See last 7 days of goal tracking
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
    goal = get_goal()
    prompt = f"""
Rushabh said: '{text}'.

His main goal for today is: '{goal}'.

You are a motivational accountability buddy. Respond like a supportive, no-nonsense friend who keeps him on track and focused on that goal.
"""
    cohere_reply = get_cohere_reply(prompt)
    await update.message.reply_text(cohere_reply)


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



async def apicount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    count = api_call_count.get("cohere", 0)
    await update.message.reply_text(f"🧠 Cohere API calls used: {count}")



async def midday_goal_reminder(application):
    from telegram import Bot
    bot = Bot(token=BOT_TOKEN)
    goal = get_goal()

    if goal:
      await bot.send_message(
        chat_id=CHAT_ID,
        text=f"⏰ Midday Reminder! Your goal today is: '{goal}' Are you working on it? Let’s lock in 🔒"
    )

            
            
    else:
        await bot.send_message(
            chat_id=CHAT_ID,
            text="⏰ Midday Reminder! You haven't set a goal for today yet. Use /goal to set one!"
        )



def get_streak():
    today = datetime.date.today()
    streak = 0
    for i in range(7):
        day = today - datetime.timedelta(days=i)
        doc = goals_col.find_one({"date": day.isoformat()})
        if doc and doc.get("done") is True:
            streak += 1
        else:
            break
    return streak


def get_weekly_chart():
    today = datetime.date.today()
    emojis = []
    for i in range(6, -1, -1):
        day = today - datetime.timedelta(days=i)
        doc = goals_col.find_one({"date": day.isoformat()})
        if doc and doc.get("done") is True:
            emojis.append("✅")
        else:
            emojis.append("❌")
    return " ".join(emojis)




def get_badge(streak):
    if streak >= 30:
        return "🏆 Platinum Crown – Rushabh Mode: Unstoppable 👑"
    elif streak >= 14:
        return "🔥 Flame Badge – Certified focused beast"
    elif streak >= 7:
        return "🥇 Gold Badge – You're locked in, legend"
    elif streak >= 4:
        return "🥈 Silver Badge – Building momentum"
    elif streak >= 1:
        return "🥉 Bronze Badge – Getting warmed up"
    else:
        return "😴 No Badge Yet – Time to start a streak!"


async def streak(update: Update, context: ContextTypes.DEFAULT_TYPE):
    streak = get_streak()
    chart = get_weekly_chart()
    await update.message.reply_text(
    f"🔥 Current Streak: {streak} days in a row!\n\n📈 Weekly Progress:\n{chart}"
)




async def history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    today = datetime.date.today()
    messages = []
    for i in range(6, -1, -1):
        day = today - datetime.timedelta(days=i)
        date_str = day.strftime("%a %d %b")
        doc = goals_col.find_one({"date": day.isoformat()})
        status = "✅" if doc and doc.get("done") else "❌"
        goal_text = doc.get("goal", "No goal") if doc else "No goal"
        messages.append(f"{status} {date_str}: {goal_text}")
    await update.message.reply_text("🗓️ Goal History (Last 7 Days):\n" + "\n".join(messages))



async def passive_check(application):
    from telegram import Bot
    bot = Bot(token=BOT_TOKEN)
    goal = get_goal()

    if not goal:
        await bot.send_message(
            chat_id=CHAT_ID,
            text="👀 You haven’t set a goal in a while. What’s the move, boss? Use /goal to lock one in."
        )


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

def reset_api_counter():
    global api_call_count
    api_call_count["cohere"] = 0


if __name__ == '__main__':
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_checkin_response))
    app.add_handler(CommandHandler("pause", pause))
    app.add_handler(CommandHandler("resume", resume))
    app.add_handler(CommandHandler("goal", set_goal))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("schedule", set_schedule))
    app.add_handler(CommandHandler("showschedule", show_schedule))
    app.add_handler(CommandHandler("streak", streak))
    app.add_handler(CommandHandler("history", history))
    app.add_handler(CommandHandler("apicount", apicount))






    # Set up daily scheduler
    scheduler = BackgroundScheduler()
    scheduler.add_job(lambda: asyncio.run(send_morning_message(app)), 'cron', hour=9, minute=00, timezone=toronto)
    # 9:05 AM - ask for goal
    scheduler.add_job(lambda: asyncio.run(ask_daily_goal(app)), 'cron', hour=9, minute=5, timezone=toronto)



    # 10:30 PM - ask if goal was completed
    scheduler.add_job(lambda: asyncio.run(ask_goal_completion(app)), 'cron', hour=22, minute=30, timezone=toronto)
    scheduler.start()

    scheduler.add_job(
    lambda: asyncio.run(weekly_report(app)),
    'cron',
    day_of_week='sun',
    hour=9,
    minute=0,
    timezone=toronto
    )

for hour in [11, 13, 15, 17, 19]:  # Adjust these times if needed
    scheduler.add_job(
        lambda: asyncio.run(send_checkin(app)),
        'cron',
        hour=hour,
        minute=0,
        timezone=toronto
    )
    
    scheduler.add_job(
        lambda: asyncio.run(passive_check(app)),
        trigger="interval",
        hours=5
)

    scheduler.add_job(
        lambda: asyncio.run(midday_goal_reminder(app)),
        trigger='cron',
        hour=13,
        minute=30,
        timezone=toronto
)


    
    scheduler.add_job(
        reset_api_counter,
        'cron',
        day=1,
        hour=0,
        minute=0,
        timezone=toronto
)



    print("BRBot is running with scheduled morning message... 🌞")
    app.run_polling()
