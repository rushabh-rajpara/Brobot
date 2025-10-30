# brobot_v2.py
# Rushabh's state-aware discipline bot

import os
import random
import datetime as dt
from datetime import timedelta
from zoneinfo import ZoneInfo

from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardRemove,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
    MessageHandler,
    filters,
)

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pymongo import MongoClient, ASCENDING, DESCENDING

import cohere

# =========================
# ENV + CLIENTS
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")
COHERE_API_KEY = os.getenv("COHERE_API_KEY")
CHAT_ID = int(os.getenv("CHAT_ID", "0"))  # optional if you only DM yourself
TZ = os.getenv("TZ", "America/Toronto")
TZINFO = ZoneInfo(TZ)

if not (BOT_TOKEN and MONGO_URI and COHERE_API_KEY):
    raise RuntimeError("Missing one of BOT_TOKEN / MONGO_URI / COHERE_API_KEY env vars")

co = cohere.Client(COHERE_API_KEY)
mongo = MongoClient(MONGO_URI)
db = mongo["Brobot"]

# Collections (fresh schema)
users = db["users"]              # {user_id, name, streak, missed_days, checkin_hour, created_at}
goals = db["goals"]              # {user_id, goal, why, updated_at}
logs = db["logs"]                # {user_id, ts, kind, data}
state = db["state"]              # {user_id, mood, energy, focus, cooldown_until, last_checkin}

# Indexes (idempotent)
users.create_index([("user_id", ASCENDING)], unique=True)
goals.create_index([("user_id", ASCENDING), ("goal", ASCENDING)], unique=True)
logs.create_index([("user_id", ASCENDING), ("ts", DESCENDING)])
state.create_index([("user_id", ASCENDING)], unique=True)

# =========================
# UTILITIES
# =========================
def now():
    return dt.datetime.now(TZINFO)

def ensure_user(user_id, name):
    users.update_one(
        {"user_id": user_id},
        {
            "$setOnInsert": {
                "user_id": user_id,
                "name": name,
                "streak": 0,
                "missed_days": 0,
                "checkin_hour": 8,
                "created_at": now(),
            }
        },
        upsert=True,
    )
    state.update_one(
        {"user_id": user_id},
        {"$setOnInsert": {
            "user_id": user_id,
            "mood": None,
            "energy": None,
            "focus": None,
            "cooldown_until": None,
            "last_checkin": None,
        }},
        upsert=True,
    )

def get_tone(user_doc):
    # Mode-switching personality by performance
    streak = user_doc.get("streak", 0)
    missed = user_doc.get("missed_days", 0)
    if streak >= 5:
        return "supportive"
    if missed >= 3:
        return "tough"
    return "neutral"

def style_text(tone, msg):
    if tone == "supportive":
        return f"🔥 {msg}"
    if tone == "tough":
        return f"⚠️ {msg}"
    return f"➡️ {msg}"

def ai_reply(prompt):
    # Cohere wrapper (low temp for consistency)
    try:
        resp = co.chat(model="command-r-plus", message=prompt, temperature=0.2)
        return (resp.text or "").strip()
    except Exception as e:
        return f"(Local coach) {prompt}"

def cooldown_active(user_id):
    s = state.find_one({"user_id": user_id}) or {}
    cu = s.get("cooldown_until")
    return cu is not None and now() < cu

def set_cooldown(user_id, minutes=10):
    state.update_one(
        {"user_id": user_id},
        {"$set": {"cooldown_until": now() + timedelta(minutes=minutes)}},
        upsert=True,
    )

def log_event(user_id, kind, data=None):
    logs.insert_one({
        "user_id": user_id,
        "ts": now(),
        "kind": kind,   # checkin|mood|done|skip|reason|insight|override
        "data": data or {},
    })

def set_goal_why(user_id, goal, why):
    goals.update_one(
        {"user_id": user_id, "goal": goal},
        {"$set": {"why": why, "updated_at": now()}},
        upsert=True,
    )

def get_first_goal(user_id):
    return goals.find_one({"user_id": user_id})

def get_why(user_id, goal):
    doc = goals.find_one({"user_id": user_id, "goal": goal})
    return (doc or {}).get("why")

def bump_streak(user_id, delta=1):
    users.update_one(
        {"user_id": user_id},
        {"$inc": {"streak": delta}, "$set": {"missed_days": 0}},
        upsert=True,
    )

def bump_missed(user_id, delta=1):
    users.update_one(
        {"user_id": user_id},
        {"$inc": {"missed_days": delta}},
        upsert=True,
    )

def mood_buttons():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("😴 Tired", callback_data="mood:tired"),
         InlineKeyboardButton("🐒 Distracted", callback_data="mood:distracted")],
        [InlineKeyboardButton("⚡ Anxious", callback_data="mood:anxious"),
         InlineKeyboardButton("✅ Fine", callback_data="mood:fine")]
    ])

def action_buttons(goal):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"✅ I did {goal}", callback_data=f"done:{goal}")],
        [InlineKeyboardButton("🙅 Skip (give reason)", callback_data=f"skip:{goal}")],
        [InlineKeyboardButton("🆘 Emergency Override", callback_data=f"override:{goal}")]
    ])

def tiny_steps(mood, goal):
    if mood == "tired":
        return f"Stand up. 3 deep breaths. Splash water. Then 2-minute start on {goal}."
    if mood == "distracted":
        return f"Close all tabs. Phone face-down. 10-minute timer. Start {goal} now."
    if mood == "anxious":
        return f"Inhale 4, hold 4, exhale 6 ×6. Then 1 micro-task for {goal}."
    return f"No fluff. Start {goal}. Timer now."

def praise_line(streak):
    options = [
        "Momentum > motivation.",
        "You showed up. That’s the game.",
        "Nice. Dopamine well spent.",
        "One rep closer to the future you want.",
    ]
    if streak >= 3:
        options += ["Streak is heating up.", "You’re compounding discipline."]
    if streak >= 7:
        options += ["Certified menace to procrastination.", "Your future self is slow-clapping."]
    return random.choice(options)

# =========================
# COMMANDS
# =========================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user.id, user.full_name or user.username or "human")
    msg = (
        "Brobot v2 online — your state-aware coach.\n\n"
        "Add a goal with a personal reason:\n"
        "• /setgoal gym I want energy and consistency\n"
        "• /setgoal code Freedom via skills\n\n"
        "Daily check-in at 08:00 by default. Change: /checkintime 7  (0–23)\n"
        "Run the loop anytime: /checkin\n"
        "See progress: /stats"
    )
    await update.message.reply_text(msg)

async def cmd_setgoal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user.id, user.full_name or "")
    if not context.args or len(context.args) < 2:
        return await update.message.reply_text("Usage: /setgoal <goal> <your reason>")
    goal = context.args[0].lower()
    why = " ".join(context.args[1:])
    set_goal_why(user.id, goal, why)
    log_event(user.id, "why", {"goal": goal})
    await update.message.reply_text(f"Saved: {goal} → “{why}”. Use /checkin to start.")

async def cmd_checkintime(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not context.args:
        return await update.message.reply_text("Usage: /checkintime <hour 0-23>")
    try:
        hour = int(context.args[0])
        if not (0 <= hour <= 23):
            raise ValueError()
    except ValueError:
        return await update.message.reply_text("Enter an hour 0–23.")
    users.update_one({"user_id": user.id}, {"$set": {"checkin_hour": hour}}, upsert=True)
    await update.message.reply_text(f"Daily check-in set to {hour:02d}:00 {TZ}.")

async def cmd_checkin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user.id, user.full_name or "")
    g = get_first_goal(user.id)
    if not g:
        return await update.message.reply_text("Set a goal first: /setgoal <goal> <why>")
    state.update_one({"user_id": user.id}, {"$set": {"last_checkin": now()}}, upsert=True)
    await update.message.reply_text(
        f"Check-in for **{g['goal']}**. How are you right now?",
        reply_markup=mood_buttons(),
        parse_mode="Markdown",
    )
    log_event(user.id, "checkin", {"goal": g["goal"], "manual": True})

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    u = users.find_one({"user_id": user.id}) or {}
    s = state.find_one({"user_id": user.id}) or {}
    gcount = goals.count_documents({"user_id": user.id})
    streak = u.get("streak", 0)
    missed = u.get("missed_days", 0)
    last10 = list(logs.find({"user_id": user.id}).sort("ts", DESCENDING).limit(10))
    lines = [
        f"Goals: {gcount} | Streak: {streak} | MissedDays: {missed}",
        f"Last mood: {s.get('mood') or 'n/a'} | Cooldown: {'on' if cooldown_active(user.id) else 'off'}",
        "Recent:"
    ]
    for L in last10:
        t = L["ts"].astimezone(TZINFO).strftime("%b %d %H:%M")
        lines.append(f"• {t} – {L['kind']}")
    await update.message.reply_text("\n".join(lines))

async def cmd_override(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    g = get_first_goal(user.id)
    if not g:
        return await update.message.reply_text("Set a goal first: /setgoal <goal> <why>")
    await run_override(user.id, g["goal"], context)

# =========================
# CALLBACKS
# =========================
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    data = query.data

    # Mood selected
    if data.startswith("mood:"):
        mood = data.split(":")[1]
        state.update_one({"user_id": user.id}, {"$set": {"mood": mood}}, upsert=True)
        g = get_first_goal(user.id)
        udoc = users.find_one({"user_id": user.id}) or {}
        tone = get_tone(udoc)
        step = tiny_steps(mood, g["goal"])
        why = get_why(user.id, g["goal"])
        msg = style_text(tone, f"{step}\n\nYour why: “{why or '—'}”.")
        await query.edit_message_text(msg, reply_markup=action_buttons(g["goal"]))
        log_event(user.id, "mood", {"mood": mood})
        return

    # Done
    if data.startswith("done:"):
        goal = data.split(":")[1]
        bump_streak(user.id, 1)
        udoc = users.find_one({"user_id": user.id}) or {}
        line = praise_line(udoc.get("streak", 0))
        log_event(user.id, "done", {"goal": goal})
        await query.edit_message_text(f"✅ Logged: {goal}. {line}")
        return

    # Skip → friction + ask reason
    if data.startswith("skip:"):
        goal = data.split(":")[1]
        set_cooldown(user.id, minutes=10)
        bump_missed(user.id, 1)
        log_event(user.id, "skip", {"goal": goal})
        await query.edit_message_text(
            "Skip noted. Entertainment cooldown: 10 min.\nWhat’s the reason?",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Cancel", callback_data="cancel_reason")]])
        )
        context.user_data["awaiting_reason_for"] = goal
        return

    if data == "cancel_reason":
        context.user_data.pop("awaiting_reason_for", None)
        await query.edit_message_text("Reason entry canceled.")
        return

    # Emergency override
    if data.startswith("override:"):
        goal = data.split(":")[1]
        await run_override(user.id, goal, context)
        await query.edit_message_text("Override initiated. Check your chat.")
        return

# Collect reason text after skip
async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    txt = (update.message.text or "").strip()

    # Cooldown gate
    if cooldown_active(user.id):
        return await update.message.reply_text("Cooldown active. Back to work; try again later.", reply_markup=ReplyKeyboardRemove())

    if "awaiting_reason_for" in context.user_data:
        goal = context.user_data.pop("awaiting_reason_for")
        logs.insert_one({
            "user_id": user.id, "ts": now(), "kind": "reason",
            "data": {"goal": goal, "reason": txt}
        })
        why = get_why(user.id, goal)
        nudge = f"You said “{why or '—'}”. Is this reason stronger than that?\nNext tiny step: {tiny_steps('distracted', goal)}"
        return await update.message.reply_text(nudge)

    # Fallback small coach via Cohere
    g = get_first_goal(user.id)
    goal = g["goal"] if g else "—"
    prompt = (
        f"User said: '{txt}'.\n"
        f"Current focus goal: '{goal}'.\n"
        "Reply as a concise, no-nonsense accountability coach. Offer a smallest next step."
    )
    reply = ai_reply(prompt)
    await update.message.reply_text(reply)

# =========================
# EMERGENCY OVERRIDE
# =========================
async def run_override(user_id, goal, context: ContextTypes.DEFAULT_TYPE):
    step1 = "Grounding: 6 cycles — inhale 4, hold 4, exhale 6. Drink water. Stand up and shake arms."
    why = get_why(user_id, goal) or "—"
    step2 = f"Your why: “{why}”."
    step3 = f"Smallest action: open the tool. If {goal == 'code'} → open VS Code; if gym → put on shoes. 90-second rule."
    log_event(user_id, "override", {"goal": goal})
    await context.bot.send_message(chat_id=user_id, text=f"{step1}\n\n{step2}\n\n{step3}")

# =========================
# SCHEDULED JOBS
# =========================
async def job_daily_checkins(app):
    # Runs hourly; messages only at each user's chosen hour
    now_local = now()
    for u in users.find({}):
        if u.get("checkin_hour", 8) == now_local.hour:
            g = get_first_goal(u["user_id"])
            if not g:
                continue
            try:
                await app.bot.send_message(
                    chat_id=u["user_id"],
                    text=f"Daily check-in for **{g['goal']}**. How are you right now?",
                    reply_markup=mood_buttons(),
                    parse_mode="Markdown",
                )
                log_event(u["user_id"], "checkin", {"goal": g["goal"], "auto": True})
            except Exception:
                pass

async def job_weekly_insights(app):
    one_week = now() - timedelta(days=7)
    for u in users.find({}):
        uid = u["user_id"]
        week = list(logs.find({"user_id": uid, "ts": {"$gte": one_week}}))
        done = sum(1 for L in week if L["kind"] == "done")
        skip = sum(1 for L in week if L["kind"] == "skip")
        mood_counts = {}
        for L in week:
            if L["kind"] == "mood":
                m = (L["data"] or {}).get("mood")
                if m:
                    mood_counts[m] = mood_counts.get(m, 0) + 1
        top_mood = max(mood_counts, key=mood_counts.get) if mood_counts else "n/a"

        msg = (
            "📊 Weekly Insight\n"
            f"• Done: {done} | Skips: {skip}\n"
            f"• Most frequent state: {top_mood}\n"
            "• Suggestion: stack your hardest task right after your natural energy peak.\n"
            "Reply /stats for last events."
        )
        try:
            await app.bot.send_message(chat_id=uid, text=msg)
            log_event(uid, "insight", {"done": done, "skip": skip, "top_mood": top_mood})
        except Exception:
            pass

# =========================
# APP WIRING
# =========================
async def on_start(app):
    scheduler = AsyncIOScheduler(timezone=TZ)
    # Hourly tick → per-user check-in at their hour
    scheduler.add_job(lambda: job_daily_checkins(app), "cron", minute=0)
    # Weekly insights: Sunday 18:00 local
    scheduler.add_job(lambda: job_weekly_insights(app), "cron", day_of_week="sun", hour=18, minute=0)
    scheduler.start()

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("setgoal", cmd_setgoal))
    app.add_handler(CommandHandler("checkintime", cmd_checkintime))
    app.add_handler(CommandHandler("checkin", cmd_checkin))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("override", cmd_override))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))

    app.post_init = on_start
    app.run_polling()

if __name__ == "__main__":
    main()
