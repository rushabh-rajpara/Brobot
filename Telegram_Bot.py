# main.py
import os
import random
import asyncio
import datetime as dt
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Any
from pymongo.errors import PyMongoError


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
sessions = db["sessions"]  # { user_id, goal, state, timebox_min, started_at, ends_at, evidence_score, last_nudge_at, created_at }
events   = db["events"]    # optional: raw passive events you’ll ingest later

started_confirmed: bool
nudges_sent: int
next_check_at: datetime
asked_completion: bool
positive_minutes: int



# Indexes
users.create_index([("user_id", ASCENDING)], unique=True)
goals.create_index([("user_id", ASCENDING), ("goal", ASCENDING)], unique=True)
logs.create_index([("user_id", ASCENDING), ("ts", DESCENDING)])
state.create_index([("user_id", ASCENDING)], unique=True)
sessions.create_index([("user_id", ASCENDING), ("state", ASCENDING), ("started_at", DESCENDING)])
events.create_index([("user_id", ASCENDING), ("ts", DESCENDING)])

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

def get_current_goal(user_id: int):
    """Use active goal if you added that feature; otherwise fallback to first goal."""
    try:
        # If you implemented active-goal helpers, prefer them:
        g = goals.find_one({"user_id": user_id, "goal": (users.find_one({"user_id": user_id}) or {}).get("active_goal")})
        if g:
            return g
    except Exception:
        pass
    return get_first_goal(user_id)

def start_session(user_id: int, timebox_min: int, goal: str | None = None) -> str:
    g = goal or ((get_current_goal(user_id) or {}).get("goal"))
    if not g:
        raise ValueError("No goal set for user.")
    ends = now() + timedelta(minutes=timebox_min)
    doc = {
        "user_id": user_id,
        "goal": g,
        "state": "ACTIVE",  # PLANNED|ACTIVE|DONE|TIMEOUT|ABORTED
        "timebox_min": int(timebox_min),
        "started_at": now(),
        "ends_at": ends,
        "evidence_score": 0.0,
        "last_nudge_at": None,
        "created_at": now(),
    }
    res = sessions.insert_one(doc)
    log_event(user_id, "session_start", {"goal": g, "timebox_min": timebox_min, "sid": str(res.inserted_id)})
    return str(res.inserted_id)

def finish_latest_session(user_id: int, state: str = "DONE") -> bool:
    """Mark the most recent ACTIVE session as DONE/TIMEOUT/ABORTED."""
    s = sessions.find_one({"user_id": user_id, "state": "ACTIVE"}, sort=[("started_at", DESCENDING)])
    if not s:
        return False
    sessions.update_one({"_id": s["_id"]}, {"$set": {"state": state, "ends_at": now()}})
    log_event(user_id, "session_finish", {"sid": str(s["_id"]), "to": state})
    return True

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

def get_active_goal(user_id: int):
    u = users.find_one({"user_id": user_id}) or {}
    active = u.get("active_goal")
    if active:
        g = goals.find_one({"user_id": user_id, "goal": active})
        if g: 
            return g
    return goals.find_one({"user_id": user_id})  # fallback

def set_active_goal(user_id: int, goal: str) -> bool:
    g = goals.find_one({"user_id": user_id, "goal": goal})
    if not g:
        return False
    users.update_one({"user_id": user_id}, {"$set": {"active_goal": goal}}, upsert=True)
    return True

def goals_list_buttons(user_id: int):
    items = list(goals.find({"user_id": user_id}))
    rows = []
    for g in items:
        rows.append([InlineKeyboardButton(f"Set active: {g['goal']}", callback_data=f"active:{g['goal']}")])
    return InlineKeyboardMarkup(rows or [[InlineKeyboardButton("No goals set", callback_data="noop")]])

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

# === /focus command ===
async def cmd_focus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user.id, user.full_name or user.username or "human")
    if not context.args:
        return await update.message.reply_text("Usage: /focus <minutes>  (e.g., /focus 25)")
    try:
        mins = int(context.args[0])
        if mins <= 0 or mins > 240:
            raise ValueError()
    except ValueError:
        return await update.message.reply_text("Enter a valid number of minutes (1–240).")
    g = get_current_goal(user.id)
    if not g:
        return await update.message.reply_text("Set a goal first: /setgoal <goal> <why>")
    try:
        sid = start_session(user.id, mins, g["goal"])
        sessions.update_one(
            {"_id": sessions.find_one({"_id": sessions._BaseObject__codec_options.document_class(sid)})["_id"]} if False else {"_id": sessions.find_one({"user_id": user.id, "state": "ACTIVE"}, sort=[("started_at", DESCENDING)])["_id"]},
            {"$set": {
                "started_confirmed": False,
                "nudges_sent": 0,
                "next_check_at": now() + timedelta(minutes=5),
                "asked_completion": False,
                "positive_minutes": 0,
            }}
        )
    except Exception as e:
        return await update.message.reply_text(f"Could not start session: {e}")
    end_local = (now() + timedelta(minutes=mins)).astimezone(TZINFO).strftime("%H:%M")
    await update.message.reply_text(
        f"🎯 Focus session started for **{g['goal']}** — {mins} min. Ends ~{end_local}.\n"
        f"I’ll check in at +5 min.", parse_mode="Markdown"
    )

async def cmd_setgoal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user.id, user.full_name or "")
    if not context.args or len(context.args) < 2:
        return await update.message.reply_text("Usage: /setgoal <goal> <your reason>")
    goal = context.args[0].lower()
    why = " ".join(context.args[1:])
    set_goal_why(user.id, goal, why)
    # set active if none exists
    u = users.find_one({"user_id": user.id}) or {}
    if not u.get("active_goal"):
        users.update_one({"user_id": user.id}, {"$set": {"active_goal": goal}}, upsert=True)
    log_event(user.id, "why", {"goal": goal})
    await update.message.reply_text(f"Saved: {goal} → “{why}”. Active goal: {goal}. Use /checkin to start.")

async def cmd_setactive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user.id, user.full_name or "")
    if not context.args:
        return await update.message.reply_text("Usage: /setactive <goal>")
    goal = context.args[0].lower()
    ok = set_active_goal(user.id, goal)
    if not ok:
        return await update.message.reply_text(f"No such goal: {goal}. Use /goals to see yours.")
    await update.message.reply_text(f"Active goal set to: {goal}")

async def cmd_goals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    items = list(goals.find({"user_id": user.id}))
    if not items:
        return await update.message.reply_text("No goals yet. Add one: /setgoal <goal> <why>")
    u = users.find_one({"user_id": user.id}) or {}
    active = u.get("active_goal")
    lst = "\n".join([f"• {g['goal']}" + ("  ← active" if g['goal']==active else "") for g in items])
    await update.message.reply_text(f"Your goals:\n{lst}", reply_markup=goals_list_buttons(user.id))

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
    g = get_active_goal(user.id)
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
    
    # Set active goal from inline button
    if data.startswith("active:"):
        goal = data.split(":")[1]
        if set_active_goal(user.id, goal):
            await query.edit_message_text(f"Active goal set to: {goal}")
        else:
            await query.edit_message_text("Could not set active goal.")
        return
    
        # === PHASE 1: session callback handlers ===
    if data == "sess:start_yes":
        s = sessions.find_one({"user_id": user.id, "state": "ACTIVE"}, sort=[("started_at", DESCENDING)])
        if s:
            sessions.update_one({"_id": s["_id"]}, {"$set": {"started_confirmed": True, "next_check_at": now() + timedelta(minutes=15)}})
        await query.edit_message_text("Locked in. Next check at +15. Keep swinging. 🔥")
        return

    if data == "sess:start_no":
        s = sessions.find_one({"user_id": user.id, "state": "ACTIVE"}, sort=[("started_at", DESCENDING)])
        if s:
            sessions.update_one({"_id": s["_id"]}, {"$set": {"next_check_at": now() + timedelta(minutes=5)}})
        await query.edit_message_text("No shame—start the tiniest step. Timer in 5. ⏱️")
        return

    if data == "sess:still_yes":
        s = sessions.find_one({"user_id": user.id, "state": "ACTIVE"}, sort=[("started_at", DESCENDING)])
        if s:
            sessions.update_one({"_id": s["_id"]}, {"$set": {"next_check_at": now() + timedelta(minutes=15)}})
        await query.edit_message_text("Nice—momentum > motivation. I’ll ping later. ⚡")
        return

    if data == "sess:still_no":
        s = sessions.find_one({"user_id": user.id, "state": "ACTIVE"}, sort=[("started_at", DESCENDING)])
        if s:
            sessions.update_one({"_id": s["_id"]}, {"$set": {"next_check_at": now() + timedelta(minutes=5)}})
        await query.edit_message_text("Reset the board: one micro-task, 5-min timer. You’ve got this. 🔁")
        return

    if data == "sess:complete_yes":
        ok = finish_latest_session(user.id, state="DONE")
        await query.edit_message_text("🏁 Session marked done. Save the win and breathe. 🙌")
        return

    if data == "sess:complete_no":
        s = sessions.find_one({"user_id": user.id, "state": "ACTIVE"}, sort=[("started_at", DESCENDING)])
        if s:
            sessions.update_one({"_id": s["_id"]}, {"$set": {"asked_completion": True, "next_check_at": now() + timedelta(minutes=5)}})
        await query.edit_message_text("All good. 5 more minutes. Then we reassess. ⏳")
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

def _hour_bucket(dt_utc): return dt_utc.strftime("%Y-%m-%dT%H")

async def cron_daily(app: Application):
    """Send check-ins to users at their chosen hour."""
    now_utc = dt.datetime.now(dt.timezone.utc)
    for u in users.find({}):
        try:
            local_hour = now_utc.astimezone(ZoneInfo(u.get("tz", TZ))).hour
            if local_hour != u.get("checkin_hour", 8):
                continue
            bucket = _hour_bucket(now_utc)
            if u.get("last_daily_sent") == bucket:
                continue
            g = get_first_goal(u["user_id"])
            if not g:
                continue
            await app.bot.send_message(u["user_id"],
            text=f"Daily check-in for **{g['goal']}**. How are you right now?",
            reply_markup=mood_buttons(), parse_mode="Markdown")
            users.update_one({"user_id": u["user_id"]}, {"$set": {"last_daily_sent": bucket}})
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
tg_app.add_handler(CommandHandler("setactive", cmd_setactive))
tg_app.add_handler(CommandHandler("goals", cmd_goals))
tg_app.add_handler(CommandHandler("focus", cmd_focus))

tg_app.add_handler(CallbackQueryHandler(on_callback))
tg_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))

@app.on_event("startup")
async def verify_dependencies():
    await tg_app.initialize()
    try:
        mongo.admin.command("ping")
        print("[startup] Mongo ok")
    except PyMongoError as e:
        raise RuntimeError(f"Mongo ping failed: {e}")
    
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

# === PHASE 0: simple API (protected by ?secret=CRON_SECRET) ===
def _require_api_secret(req: Request):
    if not CRON_SECRET:
        raise HTTPException(status_code=503, detail="API secret not configured")
    if req.query_params.get("secret") != CRON_SECRET:
        raise HTTPException(status_code=401, detail="Invalid secret")

@app.post("/sessions/start")
async def api_sessions_start(request: Request):
    _require_api_secret(request)
    data = await request.json()
    try:
        user_id = int(data.get("user_id"))
        timebox_min = int(data.get("timebox_min", 25))
        goal = (data.get("goal") or None)
    except Exception:
        raise HTTPException(status_code=400, detail="user_id and timebox_min are required")
    ensure_user(user_id, "api")
    try:
        sid = start_session(user_id, timebox_min, goal)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"{e}")
    return {"ok": True, "session_id": sid}

@app.post("/sessions/finish")
async def api_sessions_finish(request: Request):
    _require_api_secret(request)
    data = await request.json()
    user_id = int(data.get("user_id"))
    state = data.get("state", "DONE")
    if state not in ("DONE", "TIMEOUT", "ABORTED"):
        raise HTTPException(status_code=400, detail="state must be DONE|TIMEOUT|ABORTED")
    ok = finish_latest_session(user_id, state=state)
    return {"ok": ok}

@app.post("/events")
async def api_events(request: Request):
    """Phase-0 event ingest (we'll use it in Phase-1)."""
    _require_api_secret(request)
    data = await request.json()
    # expected: { user_id, kind, value, ts? }
    try:
        uid = int(data["user_id"])
        kind = str(data["kind"])
        value = data.get("value")
    except Exception:
        raise HTTPException(status_code=400, detail="user_id and kind required")
    ts = data.get("ts")
    ts_dt = now() if not ts else dt.datetime.fromisoformat(ts)
    events.insert_one({"user_id": uid, "kind": kind, "value": value, "ts": ts_dt})
    # mirror into logs for visibility
    log_event(uid, "event", {"kind": kind, "value": value})
    return {"ok": True}

# === PHASE 1: minute tick driving nudges & completion asks ===
def _session_msg_goal_line(s): return f"**{s.get('goal','—')}**"

async def cron_sessions_tick(app: Application):
    now_utc = now()
    # Look at ACTIVE sessions only
    active = list(sessions.find({"state": "ACTIVE"}))
    for s in active:
        uid = s["user_id"]
        # Ask completion when timebox is up (once)
        if now_utc >= s.get("ends_at", now_utc) and not s.get("asked_completion", False):
            try:
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Done", callback_data="sess:complete_yes"),
                     InlineKeyboardButton("⏳ Not yet", callback_data="sess:complete_no")]
                ])
                await app.bot.send_message(uid, text=f"⏱️ Time’s up for {_session_msg_goal_line(s)}. Did you finish?", reply_markup=kb, parse_mode="Markdown")
                sessions.update_one({"_id": s["_id"]}, {"$set": {"asked_completion": True}})
            except Exception:
                pass
            continue

        # If still within timebox, handle start/still-working checks
        nca = s.get("next_check_at")
        if not nca or now_utc < nca:
            continue

        nudges = int(s.get("nudges_sent", 0))
        started = bool(s.get("started_confirmed", False))

        # Build message tone (simple version; we’ll make it spicier later)
        if not started:
            txt = f"🔥 {_session_msg_goal_line(s)} — Did you start?"
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("Yes, I started", callback_data="sess:start_yes"),
                 InlineKeyboardButton("Not yet", callback_data="sess:start_no")]
            ])
            next_dt = now_utc + timedelta(minutes=5)  # keep pushing every 5 until started
        else:
            txt = f"⚡ {_session_msg_goal_line(s)} — Still in the pocket?"
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("Still working", callback_data="sess:still_yes"),
                 InlineKeyboardButton("I drifted", callback_data="sess:still_no")]
            ])
            next_dt = now_utc + timedelta(minutes=15)  # rhythm checks

        # Nudge cap to avoid spam (max 4)
        if nudges >= 4 and started:
            # Back off silently once they’re moving
            sessions.update_one({"_id": s["_id"]}, {"$set": {"next_check_at": next_dt}})
            continue

        try:
            await app.bot.send_message(uid, text=txt, reply_markup=kb, parse_mode="Markdown")
            sessions.update_one({"_id": s["_id"]}, {"$set": {"next_check_at": next_dt}, "$inc": {"nudges_sent": 1}})
        except Exception:
            pass

# Endpoint to trigger it (like your other cron endpoints)
@app.get("/cron/sessions-tick")
async def cron_sessions_tick_endpoint(request: Request):
    _check_cron_auth(request)
    await cron_sessions_tick(tg_app)
    return PlainTextResponse("sessions-tick-ok")
