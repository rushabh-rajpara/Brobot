# main.py
import os
import random
import asyncio
import datetime as dt
from datetime import timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Any

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, ContextTypes, filters
)

from pymongo import MongoClient, ASCENDING, DESCENDING
import cohere

# =========================
# ENV
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")
COHERE_API_KEY = os.getenv("COHERE_API_KEY")
TZ = os.getenv("TZ", "America/Toronto")
TZINFO = ZoneInfo(TZ)

# Security
TELEGRAM_SECRET_TOKEN = os.getenv("TELEGRAM_SECRET_TOKEN")  # for webhook header validation
CRON_SECRET = os.getenv("CRON_SECRET")                      # for /cron/* endpoints protection

if not (BOT_TOKEN and MONGO_URI and COHERE_API_KEY):
    raise RuntimeError("Missing one of BOT_TOKEN / MONGO_URI / COHERE_API_KEY")

# =========================
# CLIENTS + DB
# =========================
co = cohere.Client(COHERE_API_KEY)
mongo = MongoClient(MONGO_URI)
db = mongo["Brobot"]

users = db["users"]    # {user_id, name, streak, missed_days, checkin_hour, created_at}
goals = db["goals"]    # {user_id, goal, why, updated_at}
logs = db["logs"]      # {user_id, ts, kind, data}
state = db["state"]    # {user_id, mood, energy, focus, cooldown_until, last_checkin}

# Indexes
users.create_index([("user_id", ASCENDING)], unique=True)
goals.create_index([("user_id", ASCENDING), ("goal", ASCENDING)], unique=True)
logs.create_index([("user_id", ASCENDING), ("ts", DESCENDING)])
state.create_index([("user_id", ASCENDING)], unique=True)

# =========================
# UTIL
# =========================
def now():
    return dt.datetime.now(TZINFO)

def ensure_user(user_id: int, name: str):
    users.update_one(
        {"user_id": user_id},
        {"$setOnInsert": {
            "user_id": user_id,
            "name": name,
            "streak": 0,
            "missed_days": 0,
            "checkin_hour": 8,      # default 8am local
            "created_at": now(),
        }},
        upsert=True
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
        upsert=True
    )

def get_tone(user_doc: Dict[str, Any]) -> str:
    streak = user_doc.get("streak", 0)
    missed = user_doc.get("missed_days", 0)
    if streak >= 5:
        return "supportive"
    if missed >= 3:
        return "tough"
    return "neutral"

def style_text(tone: str, msg: str) -> str:
    if tone == "supportive":
        return f"🔥 {msg}"
    if tone == "tough":
        return f"⚠️ {msg}"
    return f"➡️ {msg}"

def ai_reply(prompt: str) -> str:
    try:
        resp = co.chat(model="command-r-plus", message=prompt, temperature=0.2)
        return (resp.text or "").strip()
    except Exception:
        return prompt

def cooldown_active(user_id: int) -> bool:
    s = state.find_one({"user_id": user_id}) or {}
    cu = s.get("cooldown_until")
    return cu is not None and now() < cu

def set_cooldown(user_id: int, minutes: int = 10):
    state.update_one(
        {"user_id": user_id},
        {"$set": {"cooldown_until": now() + timedelta(minutes=minutes)}},
        upsert=True
    )

def log_event(user_id: int, kind: str, data: Dict[str, Any] | None = None):
    logs.insert_one({
        "user_id": user_id,
        "ts": now(),
        "kind": kind,   # checkin|mood|done|skip|reason|insight|override
        "data": data or {}
    })

def set_goal_why(user_id: int, goal: str, why: str):
    goals.update_one(
        {"user_id": user_id, "goal": goal},
        {"$set": {"why": why, "updated_at": now()}},
        upsert=True
    )

def get_first_goal(user_id: int):
    return goals.find_one({"user_id": user_id})

def get_why(user_id: int, goal: str) -> str | None:
    doc = goals.find_one({"user_id": user_id, "goal": goal})
    return (doc or {}).get("why")

def bump_streak(user_id: int, delta: int = 1):
    users.update_one(
        {"user_id": user_id},
        {"$inc": {"streak": delta}, "$set": {"missed_days": 0}},
        upsert=True
    )

def bump_missed(user_id: int, delta: int = 1):
    users.update_one({"user_id": user_id}, {"$inc": {"missed_days": delta}}, upsert=True)

def mood_buttons():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("😴 Tired", callback_data="mood:tired"),
         InlineKeyboardButton("🐒 Distracted", callback_data="mood:distracted")],
        [InlineKeyboardButton("⚡ Anxious", callback_data="mood:anxious"),
         InlineKeyboardButton("✅ Fine", callback_data="mood:fine")]
    ])

def action_buttons(goal: str):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"✅ I did {goal}", callback_data=f"done:{goal}")],
        [InlineKeyboardButton("🙅 Skip (give reason)", callback_data=f"skip:{goal}")],
        [InlineKeyboardButton("🆘 Emergency Override", callback_data=f"override:{goal}")]
    ])

def tiny_steps(mood: str, goal: str) -> str:
    if mood == "tired":
        return f"Stand up. 3 deep breaths. Splash water. Then 2-minute start on {goal}."
    if mood == "distracted":
        return f"Close all tabs. Phone face-down. 10-minute timer. Start {goal} now."
    if mood == "anxious":
        return f"Inhale 4, hold 4, exhale 6 ×6. Then 1 micro-task for {goal}."
    return f"No fluff. Start {goal}. Timer now."

def praise_line(streak: int) -> str:
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
# TELEGRAM HANDLERS
# =========================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user.id, user.full_name or user.username or "human")
    msg = (
        "Brobot v2 online — your state-aware coach.\n\n"
        "Add a goal with a personal reason:\n"
        "• /setgoal gym I want energy and consistency\n"
        "• /setgoal code Freedom via skills\n\n"
        "Daily check-in at 08:00 local by default. Change: /checkintime 7  (0–23)\n"
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
        parse_mode="Markdown"
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
            "Skip noted. Entertainment cooldown: 10 min.\nWhat’s the reason?"
        )
        context.user_data["awaiting_reason_for"] = goal
        return

    # Emergency override
    if data.startswith("override:"):
        goal = data.split(":")[1]
        await run_override(user.id, goal, context)
        await query.edit_message_text("Override initiated. Check your chat.")
        return

async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    txt = (update.message.text or "").strip()

    if cooldown_active(user.id):
        return await update.message.reply_text("Cooldown active. Back to work; try again later.")

    if "awaiting_reason_for" in context.user_data:
        goal = context.user_data.pop("awaiting_reason_for")
        logs.insert_one({
            "user_id": user.id, "ts": now(), "kind": "reason",
            "data": {"goal": goal, "reason": txt}
        })
        why = get_why(user.id, goal)
        nudge = f"You said “{why or '—'}”. Is this reason stronger than that?\nNext tiny step: {tiny_steps('distracted', goal)}"
        return await update.message.reply_text(nudge)

    g = get_first_goal(user.id)
    goal = g["goal"] if g else "—"
    prompt = (
        f"User said: '{txt}'.\n"
        f"Current focus goal: '{goal}'.\n"
        "Reply as a concise, no-nonsense accountability coach. Offer a smallest next step."
    )
    reply = ai_reply(prompt)
    await update.message.reply_text(reply)

async def run_override(user_id: int, goal: str, context: ContextTypes.DEFAULT_TYPE):
    step1 = "Grounding: 6 cycles — inhale 4, hold 4, exhale 6. Drink water. Stand up and shake arms."
    why = get_why(user_id, goal) or "—"
    step2 = f"Your why: “{why}”."
    step3 = f"Smallest action: open the tool. If {goal == 'code'} → open VS Code; if gym → put on shoes. 90-second rule."
    log_event(user_id, "override", {"goal": goal})
    await context.bot.send_message(chat_id=user_id, text=f"{step1}\n\n{step2}\n\n{step3}")

# =========================
# CRON TASKS (hit by Cloudflare Cron)
# =========================
async def cron_daily(app: Application):
    """Send check-ins to users at their chosen hour."""
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

async def cron_weekly(app: Application):
    """Send weekly insight summary."""
    one_week = now() - timedelta(days=7)
    for u in users.find({}):
        uid = u["user_id"]
        week = list(logs.find({"user_id": uid, "ts": {"$gte": one_week}}))
        done = sum(1 for L in week if L["kind"] == "done")
        skip = sum(1 for L in week if L["kind"] == "skip")
        mood_counts: Dict[str, int] = {}
        for L in week:
            if L["kind"] == "mood":
                m = (L.get("data") or {}).get("mood")
                if m:
                    mood_counts[m] = mood_counts.get(m, 0) + 1
        top_mood = max(mood_counts, key=mood_counts.get) if mood_counts else "n/a"

        msg = (
            "📊 Weekly Insight\n"
            f"• Done: {done} | Skips: {skip}\n"
            f"• Most frequent state: {top_mood}\n"
            "• Suggestion: stack your hardest task right after your natural energy peak.\n"
            "Reply /stats for recent events."
        )
        try:
            await app.bot.send_message(chat_id=uid, text=msg)
            log_event(uid, "insight", {"done": done, "skip": skip, "top_mood": top_mood})
        except Exception:
            pass

# =========================
# FASTAPI WIRING
# =========================
app = FastAPI(title="Brobot v2 (webhook)")

# Build Telegram application once
tg_app: Application = ApplicationBuilder().token(BOT_TOKEN).concurrent_updates(True).build()

# Handlers
tg_app.add_handler(CommandHandler("start", cmd_start))
tg_app.add_handler(CommandHandler("setgoal", cmd_setgoal))
tg_app.add_handler(CommandHandler("checkintime", cmd_checkintime))
tg_app.add_handler(CommandHandler("checkin", cmd_checkin))
tg_app.add_handler(CommandHandler("stats", cmd_stats))
tg_app.add_handler(CommandHandler("override", cmd_override))
tg_app.add_handler(CallbackQueryHandler(on_callback))
tg_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))

@app.on_event("startup")
async def on_startup():
     await tg_app.initialize() 
    
@app.on_event("shutdown")
async def on_shutdown():
    await tg_app.shutdown()   
    
@app.get("/health")
async def health():
    return PlainTextResponse("ok")

@app.post("/webhook")
async def telegram_webhook(request: Request):
    # Validate Telegram secret token if provided
    if TELEGRAM_SECRET_TOKEN:
        hdr = request.headers.get("x-telegram-bot-api-secret-token")
        if hdr != TELEGRAM_SECRET_TOKEN:
            raise HTTPException(status_code=401, detail="Invalid telegram secret token")
    data = await request.json()
    update = Update.de_json(data=data, bot=tg_app.bot)
    await tg_app.process_update(update)
    return JSONResponse({"status": "processed"})

# Protected cron endpoints (hit these via Cloudflare Cron or any scheduler)
def _check_cron_auth(req: Request):
    if not CRON_SECRET:
        return
    q = req.query_params.get("secret")
    if q != CRON_SECRET:
        raise HTTPException(status_code=401, detail="Invalid cron secret")

@app.get("/cron/daily")
async def cron_daily_endpoint(request: Request):
    _check_cron_auth(request)
    await cron_daily(tg_app)
    return PlainTextResponse("daily-ok")

@app.get("/cron/weekly")
async def cron_weekly_endpoint(request: Request):
    _check_cron_auth(request)
    await cron_weekly(tg_app)
    return PlainTextResponse("weekly-ok")
