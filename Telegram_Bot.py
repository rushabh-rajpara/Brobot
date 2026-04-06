# main.py
import os
import random
import asyncio
import datetime as dt
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Any
from pymongo.errors import PyMongoError
from bson import ObjectId


from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.error import BadRequest
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
COHERE_MODEL = os.getenv("COHERE_MODEL", "command-r-08-2024")
TZ = os.getenv("TZ", "America/Toronto")
TZINFO = ZoneInfo(TZ)
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")

# Security
TELEGRAM_SECRET_TOKEN = os.getenv("TELEGRAM_SECRET_TOKEN")  # for webhook header validation
CRON_SECRET = os.getenv("CRON_SECRET")                      # for /cron/* endpoints protection

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("brobot")

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
profiles = db["profiles"]  # { user_id, timezone, push_style, work_start_hour, blockers, restart_size_min, onboarding_complete, conversation, created_at, updated_at }
daily_intentions = db["daily_intentions"]  # { user_id, date, selected_goal, target, fallback, status, timezone, created_at, updated_at }
memory = db["memory"]  # { user_id, key, value, confidence, updated_at }
intervention_outcomes = db["intervention_outcomes"]  # { user_id, ts, trigger_type, mode, blocker, responded, session_started, progress_occurred, issue_repeated, intervention_key }

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
profiles.create_index([("user_id", ASCENDING)], unique=True)
daily_intentions.create_index([("user_id", ASCENDING), ("date", ASCENDING)], unique=True)
memory.create_index([("user_id", ASCENDING), ("key", ASCENDING)], unique=True)
intervention_outcomes.create_index([("user_id", ASCENDING), ("ts", DESCENDING)])

COMMON_BLOCKERS = ["overwhelmed", "distracted", "tired", "anxious", "perfectionist"]
PUSH_STYLES = ["gentle", "firm", "ruthless"]
RESTART_SIZES = [5, 10, 15]
FOCUS_DURATIONS = [5, 10, 15, 25, 45]
PHRASING_STYLES = ["blunt", "tactical", "calm", "confrontational", "compressed"]
TIMEZONE_CHOICES = [
    "America/Toronto",
    "America/New_York",
    "America/Chicago",
    "America/Denver",
    "America/Los_Angeles",
    "UTC",
]

MODE_STYLE_CANDIDATES = {
    "starter": ["compressed", "tactical", "blunt", "calm"],
    "focus": ["tactical", "compressed", "blunt"],
    "clarity": ["tactical", "calm", "compressed"],
    "recovery": ["calm", "tactical", "compressed", "blunt"],
    "momentum": ["blunt", "compressed", "tactical"],
    "override": ["compressed", "confrontational", "blunt"],
}

# =========================
# UTIL
# =========================
def now():
    return dt.datetime.now(TZINFO)

def ensure_aware(ts: dt.datetime | None) -> dt.datetime | None:
    if ts is None:
        return None
    if ts.tzinfo is None:
        return ts.replace(tzinfo=TZINFO)
    return ts

def _slugify_goal(goal: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "-" for ch in goal.strip())
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned.strip("-") or "goal"

def get_profile(user_id: int) -> Dict[str, Any]:
    return profiles.find_one({"user_id": user_id}) or {}

def ensure_profile(user_id: int, name: str = "") -> Dict[str, Any]:
    user_doc = users.find_one({"user_id": user_id}) or {}
    profiles.update_one(
        {"user_id": user_id},
        {"$setOnInsert": {
            "user_id": user_id,
            "name": name or user_doc.get("name") or "human",
            "timezone": user_doc.get("tz") or TZ,
            "push_style": "firm",
            "work_start_hour": 9,
            "blockers": [],
            "restart_size_min": 10,
            "onboarding_complete": False,
            "conversation": None,
            "created_at": now(),
            "updated_at": now(),
        }},
        upsert=True
    )
    profile = get_profile(user_id)
    if profile.get("push_style"):
        set_memory(user_id, "preferred_tone", profile.get("push_style"), 0.9)
    return profile

def set_profile_fields(user_id: int, **fields):
    fields["updated_at"] = now()
    profiles.update_one({"user_id": user_id}, {"$set": fields}, upsert=True)
    if "push_style" in fields:
        set_memory(user_id, "preferred_tone", fields["push_style"], 0.95)
    if "blockers" in fields and isinstance(fields["blockers"], list):
        counts = {b: 1 for b in fields["blockers"]}
        set_memory(user_id, "common_blockers", counts, 0.7)

def set_profile_conversation(user_id: int, kind: str, step: str, data: Dict[str, Any] | None = None):
    set_profile_fields(user_id, conversation={"kind": kind, "step": step, "data": data or {}})

def clear_profile_conversation(user_id: int):
    set_profile_fields(user_id, conversation=None)

def get_conversation(user_id: int) -> Dict[str, Any] | None:
    return (get_profile(user_id) or {}).get("conversation")

def get_user_timezone(user_id: int) -> str:
    profile = get_profile(user_id)
    tz_name = profile.get("timezone") or (users.find_one({"user_id": user_id}) or {}).get("tz") or TZ
    try:
        ZoneInfo(tz_name)
        return tz_name
    except Exception:
        return TZ

def local_now_for_user(user_id: int) -> dt.datetime:
    return dt.datetime.now(ZoneInfo(get_user_timezone(user_id)))

def today_key_for_user(user_id: int) -> str:
    return local_now_for_user(user_id).date().isoformat()

def date_key_for_user(user_id: int, delta_days: int = 0) -> str:
    return (local_now_for_user(user_id).date() + timedelta(days=delta_days)).isoformat()

def list_user_goals(user_id: int):
    return list(goals.find({"user_id": user_id}).sort([("updated_at", DESCENDING), ("goal", ASCENDING)]))

def get_goal_by_ref(user_id: int, goal_ref: str):
    try:
        return goals.find_one({"_id": ObjectId(goal_ref), "user_id": user_id})
    except Exception:
        return goals.find_one({"user_id": user_id, "goal": goal_ref})

def resolve_current_goal(user_id: int, *, sync_active: bool = True):
    user_doc = users.find_one({"user_id": user_id}) or {}
    active_goal = user_doc.get("active_goal")
    if active_goal:
        active_doc = goals.find_one({"user_id": user_id, "goal": active_goal})
        if active_doc:
            return active_doc

    ordered_goals = list_user_goals(user_id)
    if not ordered_goals:
        return None

    chosen = ordered_goals[0]
    if sync_active and user_doc.get("active_goal") != chosen["goal"]:
        users.update_one({"user_id": user_id}, {"$set": {"active_goal": chosen["goal"]}}, upsert=True)
    return chosen

def get_today_intention(user_id: int):
    return daily_intentions.find_one({"user_id": user_id, "date": today_key_for_user(user_id)})

def get_intention_for_date(user_id: int, date_key: str):
    return daily_intentions.find_one({"user_id": user_id, "date": date_key})

def upsert_today_intention(user_id: int, **fields):
    date_key = today_key_for_user(user_id)
    payload = {
        "user_id": user_id,
        "date": date_key,
        "timezone": get_user_timezone(user_id),
        "updated_at": now(),
    }
    payload.update(fields)
    insert_defaults = {"created_at": now()}
    if "status" not in payload:
        insert_defaults["status"] = "planned"
    daily_intentions.update_one(
        {"user_id": user_id, "date": date_key},
        {"$set": payload, "$setOnInsert": insert_defaults},
        upsert=True,
    )
    return get_today_intention(user_id)

def set_memory(user_id: int, key: str, value: Any, confidence: float = 0.5):
    memory.update_one(
        {"user_id": user_id, "key": key},
        {"$set": {"value": value, "confidence": float(confidence), "updated_at": now()}},
        upsert=True,
    )

def get_memory(user_id: int, key: str, default=None):
    doc = memory.find_one({"user_id": user_id, "key": key})
    if not doc:
        return default
    return doc.get("value", default)

def increment_memory_counter(user_id: int, key: str, bucket: str, amount: int = 1, confidence: float = 0.6):
    current = get_memory(user_id, key, {}) or {}
    if not isinstance(current, dict):
        current = {}
    current[bucket] = int(current.get(bucket, 0)) + amount
    set_memory(user_id, key, current, confidence)
    return current

def top_bucket(value: Any):
    if not isinstance(value, dict) or not value:
        return None
    return max(value, key=value.get)

def recent_list_memory(user_id: int, key: str, limit: int = 5) -> list[str]:
    value = get_memory(user_id, key, []) or []
    if not isinstance(value, list):
        return []
    return [str(item) for item in value[:limit]]

def push_recent_memory(user_id: int, key: str, item: str, *, limit: int = 5, confidence: float = 0.65):
    items = recent_list_memory(user_id, key, limit=limit)
    items = [str(item)] + [existing for existing in items if existing != str(item)]
    set_memory(user_id, key, items[:limit], confidence)
    return items[:limit]

def record_intervention_outcome(
    user_id: int,
    *,
    trigger_type: str,
    mode: str,
    blocker: str | None = None,
    responded: bool = False,
    session_started: bool = False,
    progress_occurred: bool = False,
    issue_repeated: bool = False,
):
    doc = {
        "user_id": user_id,
        "ts": now(),
        "trigger_type": trigger_type,
        "mode": mode,
        "blocker": blocker,
        "responded": bool(responded),
        "session_started": bool(session_started),
        "progress_occurred": bool(progress_occurred),
        "issue_repeated": bool(issue_repeated),
        "intervention_key": f"{trigger_type}:{mode}:{blocker or 'none'}",
    }
    intervention_outcomes.insert_one(doc)
    if blocker:
        increment_memory_counter(user_id, "blocker_patterns", blocker, 1, 0.7)
    if progress_occurred:
        increment_memory_counter(user_id, "effective_intervention_modes", mode, 1, 0.8)
    if issue_repeated:
        increment_memory_counter(user_id, "goal_friction_patterns", blocker or trigger_type, 1, 0.65)
    return doc

def touch_user(user_id: int, source: str):
    ts = now()
    state.update_one({"user_id": user_id}, {"$set": {"last_user_touch_at": ts}}, upsert=True)
    set_profile_fields(user_id, last_user_touch_at=ts, last_user_touch_source=source)
    local_hour = local_now_for_user(user_id).hour
    increment_memory_counter(user_id, "time_of_day_activity", str(local_hour), 1, 0.55)

def get_state(user_id: int) -> Dict[str, Any]:
    return state.find_one({"user_id": user_id}) or {}

def get_recent_logs(user_id: int, *, kind: str | None = None, limit: int = 20):
    query = {"user_id": user_id}
    if kind:
        query["kind"] = kind
    return list(logs.find(query).sort("ts", DESCENDING).limit(limit))

def recent_avoidance_count(user_id: int) -> int:
    count = 0
    for doc in get_recent_logs(user_id, kind="loop_status", limit=10):
        status = (doc.get("data") or {}).get("status")
        if status in {"avoiding", "missed"}:
            count += 1
    return count

def get_active_session(user_id: int):
    return sessions.find_one({"user_id": user_id, "state": "ACTIVE"}, sort=[("started_at", DESCENDING)])

def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def count_docs(collection, query: Dict[str, Any]) -> int:
    try:
        return collection.count_documents(query)
    except Exception:
        return 0

def recent_cutoff(hours: int = 24) -> dt.datetime:
    return now() - timedelta(hours=hours)

def aggregate_status_counts(docs: list[Dict[str, Any]], field: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for doc in docs:
        value = doc.get(field)
        if value:
            counts[str(value)] = counts.get(str(value), 0) + 1
    return counts

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
    ensure_profile(user_id, name)
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
    return resolve_current_goal(user_id)

def start_session(user_id: int, timebox_min: int, goal: str | None = None, *, nudges_enabled: bool = True, source: str = "command") -> ObjectId:
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
        # Phase 1 fields defaulted here so they always exist
        "started_confirmed": False,
        "nudges_sent": 0,
        "next_check_at": None,
        "asked_completion": False,
        "positive_minutes": 0,
        "nudges_enabled": bool(nudges_enabled),
        "source": source,
    }
    res = sessions.insert_one(doc)
    sid = res.inserted_id
    log_event(user_id, "session_start", {"goal": g, "timebox_min": timebox_min, "sid": str(sid), "nudges_enabled": bool(nudges_enabled), "source": source})
    return sid

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
        resp = co.chat(model=COHERE_MODEL, message=prompt, temperature=0.2)
        return (resp.text or "").strip()
    except Exception:
        logger.exception("Cohere chat failed using model %s", COHERE_MODEL)
        return "Lock in. Pick the smallest useful next step and do it for 2 minutes right now."

def phrase_intervention(user_id: int, intervention: Dict[str, Any]) -> str:
    goal = intervention.get("goal") or (resolve_current_goal(user_id) or {}).get("goal", "your target")
    tone_policy = intervention.get("tone_policy", "firm")
    phrasing_style = intervention.get("phrasing_style", "tactical")
    recent_phrases = ", ".join(recent_list_memory(user_id, "recent_phrase_signatures", limit=3)) or "none"
    prompt = (
        f"You are phrasing a deterministic Telegram accountability intervention.\n"
        f"Tone policy: {tone_policy}.\n"
        f"Phrasing style: {phrasing_style}.\n"
        f"Goal: {goal}.\n"
        f"Trigger: {intervention.get('trigger')}.\n"
        f"Mode: {intervention.get('mode')}.\n"
        f"Blocker: {intervention.get('blocker') or 'none'}.\n"
        f"Action: {intervention.get('action')}.\n"
        f"Recent phrase signatures to avoid repeating: {recent_phrases}.\n"
        "Write 1-2 short Telegram-ready sentences. Keep it sharp, useful, and non-generic. Do not invent logic or extra options."
    )
    try:
        resp = co.chat(model=COHERE_MODEL, message=prompt, temperature=0.2)
        text = (resp.text or "").strip() or intervention.get("action", "Take the smallest next step now.")
    except Exception:
        logger.exception("Cohere intervention phrasing failed using model %s", COHERE_MODEL)
        text = intervention.get("action", "Take the smallest next step now.")
    signature = " ".join(text.lower().split()[:8])
    push_recent_memory(user_id, "recent_phrasing_styles", phrasing_style, limit=4, confidence=0.7)
    push_recent_memory(user_id, "recent_phrase_signatures", signature, limit=4, confidence=0.7)
    return text

def weekly_summary_facts(user_id: int) -> Dict[str, Any]:
    since = now() - timedelta(days=7)
    week_intentions = list(daily_intentions.find({"user_id": user_id, "updated_at": {"$gte": since}}).sort("date", ASCENDING))
    week_logs = list(logs.find({"user_id": user_id, "ts": {"$gte": since}}).sort("ts", ASCENDING))
    week_outcomes = list(intervention_outcomes.find({"user_id": user_id, "ts": {"$gte": since}}))

    active_statuses = {"active", "partial", "done", "reset_tomorrow"}
    days_active = sum(1 for item in week_intentions if item.get("status") in active_statuses)
    done_goals = [item.get("selected_goal") for item in week_intentions if item.get("status") == "done" and item.get("selected_goal")]
    key_wins = list(dict.fromkeys(done_goals))[:3]

    blocker_counts: Dict[str, int] = {}
    for outcome in week_outcomes:
        blocker = outcome.get("blocker")
        if blocker:
            blocker_counts[blocker] = blocker_counts.get(blocker, 0) + 1
    for entry in week_logs:
        if entry.get("kind") == "loop_status":
            status = (entry.get("data") or {}).get("status")
            if status in {"avoiding", "missed"}:
                blocker_counts[status] = blocker_counts.get(status, 0) + 1
    main_blocker_pattern = max(blocker_counts, key=blocker_counts.get) if blocker_counts else "none"

    worked_counts: Dict[str, int] = {}
    for outcome in week_outcomes:
        if outcome.get("progress_occurred"):
            mode = outcome.get("mode") or "unknown"
            worked_counts[mode] = worked_counts.get(mode, 0) + 1
    what_worked = max(worked_counts, key=worked_counts.get) if worked_counts else "starter"

    if main_blocker_pattern == "overwhelmed":
        adjustment = "Shrink tomorrow's target before you start."
    elif main_blocker_pattern == "distracted":
        adjustment = "Use a short focus block earlier in the day."
    elif main_blocker_pattern in {"tired", "missed"}:
        adjustment = "Plan a 5-minute restart instead of a full push."
    elif main_blocker_pattern == "anxious":
        adjustment = "Commit to an ugly first draft before polishing."
    elif main_blocker_pattern == "perfectionist":
        adjustment = "Ship rough work sooner and forbid polishing."
    else:
        adjustment = "Repeat the smallest restart that worked best this week."

    return {
        "days_active": days_active,
        "key_wins": key_wins,
        "main_blocker_pattern": main_blocker_pattern,
        "what_worked": what_worked,
        "adjustment": adjustment,
        "top_slump_hour": top_bucket(get_memory(user_id, "time_of_day_slumps", {})) or "none",
        "effective_style": top_bucket(get_memory(user_id, "effective_intervention_modes", {})) or what_worked,
    }

def phrase_weekly_summary(user_id: int, facts: Dict[str, Any]) -> str:
    wins = ", ".join(facts.get("key_wins") or ["none"])
    prompt = (
        "Phrase this deterministic weekly accountability summary in 4-5 short lines.\n"
        f"Days active: {facts.get('days_active', 0)}.\n"
        f"Key wins: {wins}.\n"
        f"Main blocker pattern: {facts.get('main_blocker_pattern', 'none')}.\n"
        f"What worked: {facts.get('what_worked', 'starter')}.\n"
        f"Top slump hour: {facts.get('top_slump_hour', 'none')}.\n"
        f"Adjustment for next week: {facts.get('adjustment', '')}.\n"
        "Do not invent facts. Keep it practical, sharp, and free of generic praise."
    )
    try:
        resp = co.chat(model=COHERE_MODEL, message=prompt, temperature=0.2)
        text = (resp.text or "").strip()
        if text:
            return text
    except Exception:
        logger.exception("Cohere weekly summary phrasing failed using model %s", COHERE_MODEL)
    return (
        "Weekly summary\n"
        f"Days active: {facts.get('days_active', 0)}\n"
        f"Key wins: {wins}\n"
        f"Main blocker: {facts.get('main_blocker_pattern', 'none')}\n"
        f"What worked: {facts.get('what_worked', 'starter')}\n"
        f"Top slump hour: {facts.get('top_slump_hour', 'none')}\n"
        f"Next adjustment: {facts.get('adjustment', '')}"
    )

def cooldown_active(user_id: int) -> bool:
    s = state.find_one({"user_id": user_id}) or {}
    cu = ensure_aware(s.get("cooldown_until"))
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

def log_structured(event: str, **fields):
    pairs = " ".join(f"{key}={fields[key]!r}" for key in sorted(fields))
    logger.info("event=%s %s", event, pairs)

async def safe_edit_message_text(query, text: str, *, reply_markup=None, parse_mode=None):
    try:
        await query.edit_message_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
        return True
    except BadRequest as exc:
        if "message is not modified" in str(exc).lower():
            await query.answer("Already up to date.")
            return False
        raise

def set_goal_why(user_id: int, goal: str, why: str):
    goals.update_one(
        {"user_id": user_id, "goal": goal},
        {"$set": {"why": why, "updated_at": now()}},
        upsert=True
    )

def get_first_goal(user_id: int):
    return resolve_current_goal(user_id)

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

def detect_blocker(user_id: int, explicit: str | None = None) -> str:
    if explicit in {"anxious", "scared"}:
        return "anxious"
    if explicit in COMMON_BLOCKERS:
        return explicit
    state_doc = get_state(user_id)
    mood = state_doc.get("mood")
    if mood in {"anxious", "tired", "distracted"}:
        return mood
    profile = ensure_profile(user_id)
    blockers = profile.get("blockers") or []
    return blockers[0] if blockers else "distracted"

def recent_blocked_sessions(user_id: int, limit: int = 5) -> int:
    docs = list(logs.find({"user_id": user_id, "kind": "focus_completion"}).sort("ts", DESCENDING).limit(limit))
    return sum(1 for doc in docs if (doc.get("data") or {}).get("status") == "blocked")

def recent_success_count(user_id: int, limit: int = 5) -> int:
    docs = list(logs.find({"user_id": user_id, "kind": {"$in": ["done", "focus_completion"]}}).sort("ts", DESCENDING).limit(limit))
    count = 0
    for doc in docs:
        data = doc.get("data") or {}
        if doc.get("kind") == "done" or data.get("status") in {"done", "partial"}:
            count += 1
    return count

def missed_day_severity(user_id: int) -> str:
    missed = int((users.find_one({"user_id": user_id}) or {}).get("missed_days", 0))
    if missed >= 4:
        return "critical"
    if missed >= 2:
        return "elevated"
    return "fresh"

def detect_goal_decay(user_id: int, goal: str | None = None) -> Dict[str, Any]:
    selected_goal = goal or ((get_today_intention(user_id) or {}).get("selected_goal")) or ((resolve_current_goal(user_id) or {}).get("goal"))
    if not selected_goal:
        return {"decayed": False, "goal": None, "severity": "none", "action": None}
    recent = list(daily_intentions.find({"user_id": user_id, "selected_goal": selected_goal}).sort("updated_at", DESCENDING).limit(7))
    low_progress = sum(1 for item in recent if item.get("status") in {"missed", "blocked", "partial"})
    repeated_avoidance = recent_avoidance_count(user_id)
    if low_progress >= 4 or repeated_avoidance >= 3:
        return {"decayed": True, "goal": selected_goal, "severity": "replace", "action": "replace"}
    if low_progress >= 3:
        return {"decayed": True, "goal": selected_goal, "severity": "split", "action": "split"}
    if low_progress >= 2:
        return {"decayed": True, "goal": selected_goal, "severity": "shrink", "action": "shrink"}
    return {"decayed": False, "goal": selected_goal, "severity": "none", "action": None}

def choose_tone_policy(user_id: int, trigger: str, *, blocker: str | None = None) -> str:
    recent_success = recent_success_count(user_id)
    avoidance = recent_avoidance_count(user_id)
    blocked = recent_blocked_sessions(user_id)
    severity = missed_day_severity(user_id)
    low_energy = top_bucket(get_memory(user_id, "time_of_day_slumps", {})) is not None and detect_blocker(user_id, blocker) == "tired"
    profile_style = ensure_profile(user_id).get("push_style", "firm")
    if trigger == "override":
        return "calm"
    if severity == "critical":
        return "compressed"
    if avoidance >= 3 or blocked >= 2:
        return "confrontational" if profile_style == "ruthless" else "blunt"
    if low_energy:
        return "calm"
    if recent_success >= 3:
        return "compressed" if profile_style == "firm" else "tactical"
    return {"gentle": "calm", "firm": "tactical", "ruthless": "blunt"}.get(profile_style, "tactical")

def choose_phrasing_style(user_id: int, mode: str, tone_policy: str) -> str:
    candidates = list(MODE_STYLE_CANDIDATES.get(mode, ["tactical", "compressed"]))
    if tone_policy == "confrontational":
        candidates = ["confrontational", "blunt"] + [c for c in candidates if c not in {"confrontational", "blunt"}]
    elif tone_policy == "calm":
        candidates = ["calm", "tactical"] + [c for c in candidates if c not in {"calm", "tactical"}]
    elif tone_policy == "compressed":
        candidates = ["compressed", "blunt"] + [c for c in candidates if c not in {"compressed", "blunt"}]
    recent_styles = recent_list_memory(user_id, "recent_phrasing_styles", limit=3)
    for style in candidates:
        if style not in recent_styles[:2]:
            return style
    return candidates[0]

def blocker_action(blocker: str, restart_size_min: int, goal: str) -> str:
    if blocker == "overwhelmed":
        return f"Find the smallest visible step for {goal}. Make it obvious and do only that."
    if blocker == "distracted":
        return f"Phone down. Close the noisy tabs. Start {goal} immediately for {restart_size_min} minutes."
    if blocker == "tired":
        return f"Reduce {goal} to 5 clean minutes. The win is starting, not grinding."
    if blocker == "anxious":
        return f"This does not need to be pretty. Do one ugly first step for {goal} and let it be messy."
    if blocker == "perfectionist":
        return f"No polishing. Push a rough version of {goal} and stop when it is merely usable."
    return f"Take one useful step on {goal} for {restart_size_min} minutes."

def missed_day_action(user_id: int, goal: str, restart: int) -> str:
    severity = missed_day_severity(user_id)
    if severity == "fresh":
        return f"Yesterday slipped. Reset fast: choose one useful target for {goal} and protect {restart} minutes."
    if severity == "elevated":
        return f"This is a streak wobble, not a collapse. Shrink {goal}, ignore extras, and get one honest restart block in."
    return f"Stop trying to catch up. Today's only job is a tiny restart on {goal}. Everything else waits."

def build_rescue_plan(user_id: int) -> Dict[str, str]:
    intention = get_today_intention(user_id) or {}
    goal = intention.get("selected_goal") or (resolve_current_goal(user_id) or {}).get("goal") or "your goal"
    target = intention.get("target") or f"move {goal} forward"
    smallest_win = blocker_action(detect_blocker(user_id), 5, goal)
    restart_minutes = min(int(ensure_profile(user_id).get("restart_size_min", 10)), 10)
    ignore = "Ignore polishing, side quests, and backlog guilt."
    follow_up = f"Check back in {restart_minutes} minutes."
    return {
        "goal": goal,
        "only_target": target,
        "smallest_win": smallest_win,
        "restart_session": f"Start {restart_minutes} minutes on {goal}.",
        "ignore": ignore,
        "follow_up": follow_up,
    }

def rescue_plan_text(user_id: int) -> str:
    plan = build_rescue_plan(user_id)
    return (
        "Rescue plan\n"
        f"Only target: {plan['only_target']}\n"
        f"Smallest acceptable win: {plan['smallest_win']}\n"
        f"Restart: {plan['restart_session']}\n"
        f"Ignore: {plan['ignore']}\n"
        f"Follow-up: {plan['follow_up']}"
    )

def choose_intervention(user_id: int, trigger: str, *, blocker: str | None = None, session_doc: Dict[str, Any] | None = None) -> Dict[str, Any]:
    profile = ensure_profile(user_id)
    goal_doc = resolve_current_goal(user_id)
    goal = (goal_doc or {}).get("goal", "your target")
    restart = int(profile.get("restart_size_min", 10))
    blocker_name = detect_blocker(user_id, blocker)
    mode = "starter"
    decay = detect_goal_decay(user_id, goal)

    if trigger == "no_response_after_morning_prompt":
        mode = "starter"
    elif trigger == "inactivity_after_target":
        mode = "focus"
    elif trigger == "unfinished_session":
        mode = "focus" if blocker_name in {"distracted", "perfectionist"} else "recovery"
    elif trigger == "repeated_avoidance":
        mode = "recovery"
    elif trigger == "missed_day":
        mode = "momentum"
    elif trigger == "stale_goal":
        mode = "clarity"
    elif trigger == "override":
        mode = "override"
    elif trigger == "goal_decay":
        mode = "clarity"

    if mode == "clarity":
        if decay.get("decayed"):
            if decay.get("action") == "replace":
                action = f"{goal} is dragging too much friction. Replace it or switch goals for today."
            elif decay.get("action") == "split":
                action = f"{goal} is too heavy as one lump. Split it into a smaller visible chunk."
            else:
                action = f"Shrink {goal} until it feels almost too easy to start."
        else:
            action = f"Pick the next visible win for {goal}. Name one outcome you can finish today and ignore the rest."
    elif mode == "momentum":
        action = missed_day_action(user_id, goal, restart) if trigger == "missed_day" else f"Reset cleanly today. Choose a smaller target for {goal} and protect {restart} minutes for it."
    elif mode == "override":
        action = f"Stop spiraling. Breathe, stand up, and do the smallest safe action toward {goal} right now."
    else:
        action = blocker_action(blocker_name, restart, goal)

    tone_policy = choose_tone_policy(user_id, trigger, blocker=blocker_name)
    phrasing_style = choose_phrasing_style(user_id, mode, tone_policy)

    result = {
        "trigger": trigger,
        "mode": mode,
        "blocker": blocker_name,
        "action": action,
        "goal": goal,
        "restart_size_min": restart,
        "session_id": str(session_doc["_id"]) if session_doc else None,
        "tone_policy": tone_policy,
        "phrasing_style": phrasing_style,
        "goal_decay": decay,
    }
    return result

def mood_buttons():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("😴 Tired", callback_data="mood:tired"),
         InlineKeyboardButton("🐒 Distracted", callback_data="mood:distracted")],
        [InlineKeyboardButton("⚡ Anxious", callback_data="mood:anxious"),
         InlineKeyboardButton("✅ Fine", callback_data="mood:fine")]
    ])

def blocker_choice_buttons(prefix: str = "recover"):
    rows = []
    for idx in range(0, len(COMMON_BLOCKERS), 2):
        chunk = COMMON_BLOCKERS[idx:idx + 2]
        rows.append([InlineKeyboardButton(b.title(), callback_data=f"{prefix}:blocker:{b}") for b in chunk])
    return InlineKeyboardMarkup(rows)

def action_buttons(goal: str):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"✅ I did {goal}", callback_data=f"done:{goal}")],
        [InlineKeyboardButton("🙅 Skip (give reason)", callback_data=f"skip:{goal}")],
        [InlineKeyboardButton("🆘 Emergency Override", callback_data=f"override:{goal}")]
    ])

def focus_duration_buttons():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{minutes} min", callback_data=f"focus:dur:{minutes}") for minutes in FOCUS_DURATIONS[:3]],
        [InlineKeyboardButton(f"{minutes} min", callback_data=f"focus:dur:{minutes}") for minutes in FOCUS_DURATIONS[3:]],
    ])

def focus_nudge_buttons(minutes: int):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Nudges on", callback_data=f"focus:nudges:on:{minutes}"),
         InlineKeyboardButton("No nudges", callback_data=f"focus:nudges:off:{minutes}")]
    ])

def focus_completion_buttons():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Done", callback_data="sess:end:done"),
         InlineKeyboardButton("Partial", callback_data="sess:end:partial"),
         InlineKeyboardButton("Blocked", callback_data="sess:end:blocked")]
    ])

def premium_action_buttons(user_id: int, intervention: Dict[str, Any]):
    rows = [
        [InlineKeyboardButton("Smallest step", callback_data="ux:smallest_step"),
         InlineKeyboardButton("Start 5 min", callback_data="ux:start5")],
    ]
    if len(list_user_goals(user_id)) > 1:
        rows.append([InlineKeyboardButton("Switch goal", callback_data="ux:switch_goal"),
                     InlineKeyboardButton("Not this one", callback_data="ux:not_this_one")])
    decay = intervention.get("goal_decay") or {}
    if decay.get("decayed"):
        rows.append([InlineKeyboardButton("Shrink target", callback_data="ux:shrink"),
                     InlineKeyboardButton("Replace goal", callback_data="ux:replace")])
    else:
        rows.append([InlineKeyboardButton("Shrink target", callback_data="ux:shrink"),
                     InlineKeyboardButton("I'm fried", callback_data="ux:fried")])
    rows.append([InlineKeyboardButton("Rescue me", callback_data="ux:rescue")])
    return InlineKeyboardMarkup(rows)

def morning_anchor_buttons():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Continue yesterday", callback_data="loop:morning:continue")],
        [InlineKeyboardButton("New target", callback_data="loop:morning:new"),
         InlineKeyboardButton("You choose", callback_data="loop:morning:choose")]
    ])

def midday_check_buttons():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Started", callback_data="loop:midday:started"),
         InlineKeyboardButton("Almost", callback_data="loop:midday:almost"),
         InlineKeyboardButton("Avoiding", callback_data="loop:midday:avoiding")]
    ])

def end_of_day_buttons():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Done", callback_data="loop:eod:done"),
         InlineKeyboardButton("Partial", callback_data="loop:eod:partial")],
        [InlineKeyboardButton("Missed", callback_data="loop:eod:missed"),
         InlineKeyboardButton("Reset tomorrow", callback_data="loop:eod:reset")]
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
    return resolve_current_goal(user_id)

def set_active_goal(user_id: int, goal: str) -> bool:
    g = get_goal_by_ref(user_id, goal)
    if not g:
        return False
    users.update_one({"user_id": user_id}, {"$set": {"active_goal": g["goal"]}}, upsert=True)
    return True

def goals_list_buttons(user_id: int):
    items = list_user_goals(user_id)
    rows = []
    for g in items:
        rows.append([InlineKeyboardButton(f"Set active: {g['goal']}", callback_data=f"active:{str(g['_id'])}")])
    return InlineKeyboardMarkup(rows or [[InlineKeyboardButton("No goals set", callback_data="noop")]])

def start_menu_buttons(user_id: int):
    profile = ensure_profile(user_id)
    rows = []
    if not profile.get("onboarding_complete"):
        rows.append([InlineKeyboardButton("Start setup", callback_data="ob:begin")])
    rows.append([InlineKeyboardButton("Today's intention", callback_data="intent:begin")])
    if get_today_intention(user_id):
        rows.append([InlineKeyboardButton("Start focus", callback_data="focus:begin")])
    rows.append([InlineKeyboardButton("Goals", callback_data="menu:goals"),
                 InlineKeyboardButton("Settings", callback_data="menu:settings")])
    return InlineKeyboardMarkup(rows)

def settings_buttons(user_id: int):
    profile = ensure_profile(user_id)
    label = "Resume onboarding" if not profile.get("onboarding_complete") else "Update onboarding"
    rows = [
        [InlineKeyboardButton(label, callback_data="ob:begin")],
        [InlineKeyboardButton("Set today's intention", callback_data="intent:begin")],
    ]
    if get_today_intention(user_id):
        rows.append([InlineKeyboardButton("Start focus", callback_data="focus:begin")])
    return InlineKeyboardMarkup(rows)

def timezone_buttons():
    rows = [[InlineKeyboardButton(tz.replace("America/", "").replace("_", " "), callback_data=f"ob:tz:{tz}")] for tz in TIMEZONE_CHOICES]
    rows.append([InlineKeyboardButton("Enter timezone manually", callback_data="ob:tz:manual")])
    return InlineKeyboardMarkup(rows)

def goal_more_buttons(goal_count: int):
    rows = []
    if goal_count < 3:
        rows.append([InlineKeyboardButton(f"Add goal {goal_count + 1}", callback_data="ob:goal_more:add")])
    rows.append([InlineKeyboardButton("Continue setup", callback_data="ob:goal_more:continue")])
    return InlineKeyboardMarkup(rows)

def push_style_buttons():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(style.title(), callback_data=f"ob:style:{style}") for style in PUSH_STYLES]
    ])

def work_start_buttons():
    hours = [6, 7, 8, 9, 10, 11, 12]
    rows = []
    for idx in range(0, len(hours), 3):
        chunk = hours[idx:idx + 3]
        rows.append([InlineKeyboardButton(f"{hour:02d}:00", callback_data=f"ob:work:{hour}") for hour in chunk])
    return InlineKeyboardMarkup(rows)

def blocker_buttons(selected: list[str]):
    rows = []
    for idx in range(0, len(COMMON_BLOCKERS), 2):
        chunk = COMMON_BLOCKERS[idx:idx + 2]
        row = []
        for blocker in chunk:
            prefix = "✓ " if blocker in selected else ""
            row.append(InlineKeyboardButton(f"{prefix}{blocker.title()}", callback_data=f"ob:blocker:{blocker}"))
        rows.append(row)
    rows.append([InlineKeyboardButton("Done selecting blockers", callback_data="ob:blocker_done")])
    return InlineKeyboardMarkup(rows)

def restart_size_buttons():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{minutes} min", callback_data=f"ob:restart:{minutes}") for minutes in RESTART_SIZES]
    ])

def intention_goal_buttons(user_id: int):
    items = list_user_goals(user_id)
    rows = [[InlineKeyboardButton(g["goal"], callback_data=f"intent:goal:{str(g['_id'])}")] for g in items[:3]]
    return InlineKeyboardMarkup(rows or [[InlineKeyboardButton("No goals yet", callback_data="noop")]])

def intention_action_buttons(status: str | None):
    current = (status or "planned").lower()
    if current == "done":
        rows = [[InlineKeyboardButton("Back to active", callback_data="intent:status:active")]]
    else:
        rows = [[InlineKeyboardButton("Mark done", callback_data="intent:status:done")]]
    if current in {"planned", "active", "partial", "blocked"}:
        rows.append([InlineKeyboardButton("Start focus", callback_data="focus:begin")])
    rows.append([InlineKeyboardButton("Refresh intention", callback_data="intent:refresh")])
    return InlineKeyboardMarkup(rows)

def morning_summary_text(user_id: int) -> str:
    yesterday = get_intention_for_date(user_id, date_key_for_user(user_id, -1))
    if yesterday:
        return (
            "What matters most today?\n\n"
            f"Yesterday: {yesterday.get('selected_goal') or '—'} | {yesterday.get('target') or '—'} | status: {yesterday.get('status') or 'planned'}"
        )
    return "What matters most today?"

def render_intervention_text(user_id: int, trigger: str, *, blocker: str | None = None, session_doc: Dict[str, Any] | None = None) -> str:
    intervention = choose_intervention(user_id, trigger, blocker=blocker, session_doc=session_doc)
    return phrase_intervention(user_id, intervention)

def intervention_reply_markup(user_id: int, trigger: str, *, blocker: str | None = None, session_doc: Dict[str, Any] | None = None):
    return premium_action_buttons(user_id, choose_intervention(user_id, trigger, blocker=blocker, session_doc=session_doc))

def maybe_log_goal_decay(user_id: int, goal: str | None = None):
    decay = detect_goal_decay(user_id, goal)
    if decay.get("decayed"):
        log_event(user_id, "goal_decay", decay)
    return decay

def focus_started_text(user_id: int, session_doc: Dict[str, Any]) -> str:
    end_local = ensure_aware(session_doc.get("ends_at")) or now()
    tz_name = get_user_timezone(user_id)
    end_str = end_local.astimezone(ZoneInfo(tz_name)).strftime("%H:%M")
    nudges_text = "Nudges are on." if session_doc.get("nudges_enabled", True) else "No nudges this round."
    return f"Focus session started for {session_doc['goal']} — {session_doc['timebox_min']} min. Ends around {end_str}. {nudges_text}"

def profile_summary(user_id: int) -> str:
    profile = ensure_profile(user_id)
    blockers = ", ".join(profile.get("blockers") or []) or "not set"
    goals_list = ", ".join(g["goal"] for g in list_user_goals(user_id)[:3]) or "none yet"
    return (
        f"Timezone: {profile.get('timezone', TZ)}\n"
        f"Goals: {goals_list}\n"
        f"Push style: {profile.get('push_style', 'firm')}\n"
        f"Work start: {profile.get('work_start_hour', 9):02d}:00\n"
        f"Blockers: {blockers}\n"
        f"Restart size: {profile.get('restart_size_min', 10)} min"
    )

def intention_summary(user_id: int) -> str:
    intention = get_today_intention(user_id)
    if not intention:
        return "No daily intention yet."
    return (
        f"Today's intention ({intention['date']})\n"
        f"Goal: {intention.get('selected_goal') or '—'}\n"
        f"Target: {intention.get('target') or '—'}\n"
        f"Fallback: {intention.get('fallback') or '—'}\n"
        f"Status: {intention.get('status') or 'planned'}"
    )

# =========================
# TELEGRAM HANDLERS
# =========================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user.id, user.full_name or user.username or "human")
    touch_user(user.id, "command:start")
    profile = ensure_profile(user.id, user.full_name or user.username or "human")
    if profile.get("onboarding_complete"):
        msg = (
            "Brobot v2 online.\n\n"
            "Primary flow is here in chat: set today's intention, start a focus block, and use buttons when you drift.\n\n"
            f"{intention_summary(user.id)}"
        )
    else:
        msg = (
            "Brobot v2 online.\n\n"
            "Let's get your setup dialed in so I can reduce friction instead of just yelling motivation.\n"
            "We'll collect your timezone, 1–3 goals, why they matter, push style, work start time, blockers, and restart size."
        )
    await update.message.reply_text(msg, reply_markup=start_menu_buttons(user.id))

async def cmd_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user.id, user.full_name or user.username or "human")
    touch_user(user.id, "command:settings")
    msg = "Settings\n\n" + profile_summary(user.id)
    await update.message.reply_text(msg, reply_markup=settings_buttons(user.id))

# === /focus command ===
async def cmd_focus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user.id, user.full_name or user.username or "human")
    touch_user(user.id, "command:focus")
    if not context.args:
        goal = ((get_today_intention(user.id) or {}).get("selected_goal")) or ((resolve_current_goal(user.id) or {}).get("goal"))
        if not goal:
            return await update.message.reply_text("Set a goal first with /settings or /goals.")
        return await update.message.reply_text(f"Pick a focus duration for {goal}.", reply_markup=focus_duration_buttons())
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
        sid = start_session(user.id, mins, g["goal"], nudges_enabled=True, source="command")
        sessions.update_one({"_id": sid}, {"$set": {"next_check_at": now() + timedelta(minutes=5)}})
    except Exception as e:
        return await update.message.reply_text(f"Could not start session: {e}")

    session_doc = sessions.find_one({"_id": sid}) or {"goal": g["goal"], "timebox_min": mins, "ends_at": now() + timedelta(minutes=mins), "nudges_enabled": True}
    await update.message.reply_text(focus_started_text(user.id, session_doc), reply_markup=focus_completion_buttons())

async def cmd_setgoal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user.id, user.full_name or "")
    touch_user(user.id, "command:setgoal")
    if not context.args or len(context.args) < 2:
        return await update.message.reply_text("Legacy setup command. Use /settings for the button-first flow, or `/setgoal <goal> <your reason>` for compatibility.", parse_mode="Markdown")
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
    touch_user(user.id, "command:setactive")
    if not context.args:
        return await update.message.reply_text("Legacy setup command. Use /goals to switch with buttons, or `/setactive <goal>` for compatibility.", parse_mode="Markdown")
    goal = context.args[0].lower()
    ok = set_active_goal(user.id, goal)
    if not ok:
        return await update.message.reply_text(f"No such goal: {goal}. Use /goals to see yours.")
    await update.message.reply_text(f"Active goal set to: {goal}")

async def cmd_goals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    touch_user(user.id, "command:goals")
    items = list_user_goals(user.id)
    if not items:
        return await update.message.reply_text("No goals yet. Add one: /setgoal <goal> <why>")
    u = users.find_one({"user_id": user.id}) or {}
    active = u.get("active_goal")
    lst = "\n".join([f"• {g['goal']}" + ("  ← active" if g['goal']==active else "") for g in items])
    await update.message.reply_text(f"Your goals:\n{lst}", reply_markup=goals_list_buttons(user.id))

async def cmd_checkintime(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    touch_user(user.id, "command:checkintime")
    if not context.args:
        return await update.message.reply_text("Legacy settings command. Prefer /settings, or use `/checkintime <hour 0-23>` for compatibility.", parse_mode="Markdown")
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
    touch_user(user.id, "command:checkin")
    g = resolve_current_goal(user.id)
    if not g:
        return await update.message.reply_text("Set a goal first: /setgoal <goal> <why>")
    upsert_today_intention(user.id, selected_goal=g["goal"])
    state.update_one({"user_id": user.id}, {"$set": {"last_checkin": now()}}, upsert=True)
    await update.message.reply_text(
        f"Check-in for **{g['goal']}**. How are you right now?",
        reply_markup=mood_buttons(),
        parse_mode="Markdown"
    )
    log_event(user.id, "checkin", {"goal": g["goal"], "manual": True})


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    touch_user(user.id, "command:stats")
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
    touch_user(user.id, "command:override")
    g = resolve_current_goal(user.id)
    if not g:
        return await update.message.reply_text("Set a goal first: /setgoal <goal> <why>")
    await run_override(user.id, g["goal"], context)

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    data = query.data
    ensure_user(user.id, user.full_name or user.username or "human")
    touch_user(user.id, f"callback:{data.split(':', 1)[0]}")

    if data == "noop":
        await query.answer()
        return

    if data == "menu:goals":
        items = list_user_goals(user.id)
        if not items:
            await safe_edit_message_text(query, "No goals yet. Add one with /settings or `/setgoal <goal> <why>`.", parse_mode="Markdown")
        else:
            active = (users.find_one({"user_id": user.id}) or {}).get("active_goal")
            lst = "\n".join([f"• {g['goal']}" + ("  ← active" if g['goal'] == active else "") for g in items])
            await safe_edit_message_text(query, f"Your goals:\n{lst}", reply_markup=goals_list_buttons(user.id))
        return

    if data == "menu:settings":
        await safe_edit_message_text(query, "Settings\n\n" + profile_summary(user.id), reply_markup=settings_buttons(user.id))
        return

    if data == "ob:begin":
        set_profile_conversation(user.id, "onboarding", "timezone_choice", {"goal_count": 0})
        await safe_edit_message_text(
            query,
            "Onboarding step 1/6.\nChoose your timezone, or enter it manually if it isn't listed.",
            reply_markup=timezone_buttons(),
        )
        return

    if data.startswith("ob:tz:"):
        tz_value = data.split(":", 2)[2]
        if tz_value == "manual":
            set_profile_conversation(user.id, "onboarding", "timezone_text", {"goal_count": 0})
            await safe_edit_message_text(query, "Send your timezone as an IANA string, for example `America/Toronto`.", parse_mode="Markdown")
            return
        set_profile_fields(user.id, timezone=tz_value)
        users.update_one({"user_id": user.id}, {"$set": {"tz": tz_value}}, upsert=True)
        set_profile_conversation(user.id, "onboarding", "goal_name", {"goal_count": 0})
        await safe_edit_message_text(query, "Onboarding step 2/6.\nSend goal 1 in a few words.")
        return

    if data.startswith("ob:goal_more:"):
        action = data.split(":", 2)[2]
        conversation = get_conversation(user.id) or {}
        goal_count = int((conversation.get("data") or {}).get("goal_count", goals.count_documents({"user_id": user.id})))
        if action == "add" and goal_count < 3:
            set_profile_conversation(user.id, "onboarding", "goal_name", {"goal_count": goal_count})
            await query.edit_message_text(f"Send goal {goal_count + 1} in a few words.")
            return
        set_profile_conversation(user.id, "onboarding", "push_style", {"goal_count": goal_count})
        await query.edit_message_text("Onboarding step 3/6.\nPick the push style you want from me.", reply_markup=push_style_buttons())
        return

    if data.startswith("ob:style:"):
        style = data.split(":", 2)[2]
        set_profile_fields(user.id, push_style=style)
        set_profile_conversation(user.id, "onboarding", "work_start", {})
        await query.edit_message_text("Onboarding step 4/6.\nWhat time does your workday usually start?", reply_markup=work_start_buttons())
        return

    if data.startswith("ob:work:"):
        hour = int(data.split(":", 2)[2])
        set_profile_fields(user.id, work_start_hour=hour)
        set_profile_conversation(user.id, "onboarding", "blockers", {"selected_blockers": []})
        await query.edit_message_text(
            "Onboarding step 5/6.\nPick your common blockers. Tap to toggle, then press done.",
            reply_markup=blocker_buttons([]),
        )
        return

    if data.startswith("ob:blocker:"):
        blocker = data.split(":", 2)[2]
        conversation = get_conversation(user.id) or {}
        selected = list((conversation.get("data") or {}).get("selected_blockers", []))
        if blocker in selected:
            selected.remove(blocker)
        else:
            selected.append(blocker)
        set_profile_conversation(user.id, "onboarding", "blockers", {"selected_blockers": selected})
        await query.edit_message_reply_markup(reply_markup=blocker_buttons(selected))
        return

    if data == "ob:blocker_done":
        conversation = get_conversation(user.id) or {}
        selected = list((conversation.get("data") or {}).get("selected_blockers", []))
        if not selected:
            await query.answer("Pick at least one blocker first.", show_alert=True)
            return
        set_profile_fields(user.id, blockers=selected)
        set_profile_conversation(user.id, "onboarding", "restart_size", {})
        await query.edit_message_text("Onboarding step 6/6.\nWhat's your preferred restart size?", reply_markup=restart_size_buttons())
        return

    if data.startswith("ob:restart:"):
        minutes = int(data.split(":", 2)[2])
        set_profile_fields(user.id, restart_size_min=minutes, onboarding_complete=True)
        clear_profile_conversation(user.id)
        await query.edit_message_text(
            "Setup complete.\n\n"
            + profile_summary(user.id)
            + "\n\nNext move: set today's intention so the bot can guide you with less friction.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Set today's intention", callback_data="intent:begin")]]),
        )
        return

    if data == "intent:begin":
        current = resolve_current_goal(user.id)
        if not current:
            await query.edit_message_text("Set at least one goal first with /setgoal <goal> <why>.")
            return
        goals_for_user = list_user_goals(user.id)
        if len(goals_for_user) == 1:
            goal = goals_for_user[0]["goal"]
            upsert_today_intention(user.id, selected_goal=goal, status="planned")
            set_profile_conversation(user.id, "intention", "target_text", {"selected_goal": goal})
            await safe_edit_message_text(query, f"What's today's target for {goal}?")
            return
        set_profile_conversation(user.id, "intention", "goal_pick", {})
        await safe_edit_message_text(query, "Choose the goal for today's intention.", reply_markup=intention_goal_buttons(user.id))
        return

    if data.startswith("intent:goal:"):
        goal_ref = data.split(":", 2)[2]
        goal_doc = get_goal_by_ref(user.id, goal_ref)
        if not goal_doc:
            await query.edit_message_text("I couldn't match that goal. Open the intention flow again and pick one more time.")
            return
        goal = goal_doc["goal"]
        upsert_today_intention(user.id, selected_goal=goal, status="planned")
        set_profile_conversation(user.id, "intention", "target_text", {"selected_goal": goal})
        await safe_edit_message_text(query, f"What's today's target for {goal}?")
        return

    if data.startswith("intent:status:"):
        status = data.split(":", 2)[2]
        intention = get_today_intention(user.id)
        if not intention:
            await query.edit_message_text("No daily intention found yet. Start with Today's intention first.")
            return
        if intention.get("status") == status:
            await query.answer(f"Already marked {status}.")
            return
        intention = upsert_today_intention(user.id, status=status)
        await safe_edit_message_text(
            query,
            intention_summary(user.id),
            reply_markup=intention_action_buttons(intention.get("status")),
        )
        return

    if data == "intent:refresh":
        intention = get_today_intention(user.id)
        if not intention:
            await query.edit_message_text("No daily intention found yet. Start with Today's intention first.")
            return
        await safe_edit_message_text(
            query,
            intention_summary(user.id),
            reply_markup=intention_action_buttons(intention.get("status")),
        )
        return

    if data == "focus:begin":
        current = get_today_intention(user.id) or {}
        goal = current.get("selected_goal") or (resolve_current_goal(user.id) or {}).get("goal")
        if not goal:
            await query.edit_message_text("Set a goal first, then come back to focus.")
            return
        await safe_edit_message_text(query, f"Pick a focus duration for {goal}.", reply_markup=focus_duration_buttons())
        return

    if data.startswith("focus:dur:"):
        minutes = int(data.split(":")[2])
        await safe_edit_message_text(
            query,
            f"{minutes} minutes selected.\nDo you want nudges during this session?",
            reply_markup=focus_nudge_buttons(minutes),
        )
        return

    if data.startswith("focus:nudges:"):
        _, _, nudges_value, minutes_value = data.split(":")
        minutes = int(minutes_value)
        nudges_enabled = nudges_value == "on"
        current = get_today_intention(user.id) or {}
        goal = current.get("selected_goal") or (resolve_current_goal(user.id) or {}).get("goal")
        if not goal:
            await query.edit_message_text("Set a goal first, then start a focus session.")
            return
        sid = start_session(user.id, minutes, goal, nudges_enabled=nudges_enabled, source="button")
        next_check = now() + timedelta(minutes=5)
        sessions.update_one({"_id": sid}, {"$set": {"next_check_at": next_check if nudges_enabled else None}})
        session_doc = sessions.find_one({"_id": sid}) or {"goal": goal, "timebox_min": minutes, "ends_at": now() + timedelta(minutes=minutes), "nudges_enabled": nudges_enabled}
        record_intervention_outcome(
            user.id,
            trigger_type="focus_button_start",
            mode="focus",
            blocker=None,
            responded=True,
            session_started=True,
            progress_occurred=False,
            issue_repeated=False,
        )
        await query.edit_message_text(
            focus_started_text(user.id, session_doc),
            reply_markup=focus_completion_buttons(),
        )
        return

    if data == "ux:smallest_step":
        goal = ((get_today_intention(user.id) or {}).get("selected_goal")) or ((resolve_current_goal(user.id) or {}).get("goal")) or "your target"
        await safe_edit_message_text(query, blocker_action(detect_blocker(user.id), 5, goal), reply_markup=focus_duration_buttons())
        return

    if data == "ux:start5":
        goal = ((get_today_intention(user.id) or {}).get("selected_goal")) or ((resolve_current_goal(user.id) or {}).get("goal"))
        if not goal:
            await safe_edit_message_text(query, "Set a goal first, then use the 5-minute restart.")
            return
        sid = start_session(user.id, 5, goal, nudges_enabled=False, source="ux_start5")
        session_doc = sessions.find_one({"_id": sid}) or {"goal": goal, "timebox_min": 5, "ends_at": now() + timedelta(minutes=5), "nudges_enabled": False}
        record_intervention_outcome(user.id, trigger_type="quick_restart", mode="starter", blocker=detect_blocker(user.id), responded=True, session_started=True, progress_occurred=False, issue_repeated=False)
        await safe_edit_message_text(query, focus_started_text(user.id, session_doc), reply_markup=focus_completion_buttons())
        return

    if data == "ux:shrink":
        intention = get_today_intention(user.id) or {}
        goal = intention.get("selected_goal") or ((resolve_current_goal(user.id) or {}).get("goal"))
        target = intention.get("target") or (goal and f"move {goal} forward") or "today's target"
        smaller = f"Smaller target: 1 visible move on {goal}" if goal else "Smaller target: one visible move"
        upsert_today_intention(user.id, selected_goal=goal, target=smaller, status="active")
        await safe_edit_message_text(query, f"Target shrunk.\n{smaller}", reply_markup=focus_duration_buttons())
        return

    if data == "ux:switch_goal":
        await safe_edit_message_text(query, "Switch goals with one tap.", reply_markup=goals_list_buttons(user.id))
        return

    if data == "ux:not_this_one":
        set_profile_conversation(user.id, "intention", "goal_pick", {})
        await safe_edit_message_text(query, "Pick a different goal for today.", reply_markup=intention_goal_buttons(user.id))
        return

    if data == "ux:fried":
        await safe_edit_message_text(
            query,
            render_intervention_text(user.id, "repeated_avoidance", blocker="tired"),
            reply_markup=intervention_reply_markup(user.id, "repeated_avoidance", blocker="tired"),
        )
        return

    if data == "ux:rescue":
        await safe_edit_message_text(query, rescue_plan_text(user.id), reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Start 5 min", callback_data="ux:start5"),
             InlineKeyboardButton("Switch goal", callback_data="ux:switch_goal")]
        ]))
        return

    if data == "ux:replace":
        set_profile_conversation(user.id, "intention", "goal_pick", {})
        await safe_edit_message_text(query, "This goal may be decaying. Pick a replacement goal for today.", reply_markup=intention_goal_buttons(user.id))
        return

    if data.startswith("sess:end:"):
        outcome = data.split(":")[2]
        mapped_state = {"done": "DONE", "partial": "DONE", "blocked": "ABORTED"}[outcome]
        ok = finish_latest_session(user.id, state=mapped_state)
        if outcome == "done":
            upsert_today_intention(user.id, status="done")
            log_event(user.id, "focus_completion", {"status": "done"})
            record_intervention_outcome(user.id, trigger_type="focus_completion", mode="focus", responded=True, session_started=True, progress_occurred=True, issue_repeated=False)
            await query.edit_message_text("Session logged as done. Keep the momentum.")
        elif outcome == "partial":
            upsert_today_intention(user.id, status="partial")
            log_event(user.id, "focus_completion", {"status": "partial"})
            record_intervention_outcome(user.id, trigger_type="focus_completion", mode="momentum", responded=True, session_started=True, progress_occurred=True, issue_repeated=False)
            await query.edit_message_text("Partial counts. Keep the useful pieces and reset clean.")
        else:
            upsert_today_intention(user.id, status="blocked")
            log_event(user.id, "focus_completion", {"status": "blocked"})
            record_intervention_outcome(user.id, trigger_type="focus_completion", mode="recovery", blocker=detect_blocker(user.id), responded=True, session_started=True, progress_occurred=False, issue_repeated=True)
            maybe_log_goal_decay(user.id)
            await safe_edit_message_text(
                query,
                render_intervention_text(user.id, "unfinished_session"),
                reply_markup=intervention_reply_markup(user.id, "unfinished_session"),
            )
        return

    if data == "loop:morning:continue":
        yesterday = get_intention_for_date(user.id, date_key_for_user(user.id, -1))
        if not yesterday or not yesterday.get("selected_goal"):
            await query.edit_message_text("No clean yesterday target found. Pick a new one instead.", reply_markup=intention_goal_buttons(user.id))
            set_profile_conversation(user.id, "intention", "goal_pick", {})
            return
        upsert_today_intention(
            user.id,
            selected_goal=yesterday.get("selected_goal"),
            target=yesterday.get("target"),
            fallback=yesterday.get("fallback"),
            status="active",
            morning_choice="continue_yesterday",
            morning_response_at=now(),
        )
        await query.edit_message_text(
            intention_summary(user.id),
            reply_markup=intention_action_buttons("active"),
        )
        return

    if data == "loop:morning:new":
        upsert_today_intention(user.id, morning_choice="new_target", morning_response_at=now(), status="planned")
        set_profile_conversation(user.id, "intention", "goal_pick", {})
        await query.edit_message_text("Pick the goal for today's target.", reply_markup=intention_goal_buttons(user.id))
        return

    if data == "loop:morning:choose":
        current = resolve_current_goal(user.id)
        if not current:
            await query.edit_message_text("Set a goal first with /setgoal <goal> <why>.")
            return
        upsert_today_intention(user.id, selected_goal=current["goal"], morning_choice="you_choose", morning_response_at=now(), status="planned")
        set_profile_conversation(user.id, "intention", "target_text", {"selected_goal": current["goal"]})
        await query.edit_message_text(f"Today's best bet is {current['goal']}.\nWhat's the target?")
        return

    if data == "loop:midday:started":
        upsert_today_intention(user.id, status="active", midday_status="started", midday_response_at=now())
        log_event(user.id, "loop_status", {"phase": "midday", "status": "started"})
        record_intervention_outcome(user.id, trigger_type="midday_check", mode="momentum", responded=True, session_started=False, progress_occurred=True, issue_repeated=False)
        await query.edit_message_text("Good. Protect the next block and keep moving.", reply_markup=focus_duration_buttons())
        return

    if data == "loop:midday:almost":
        upsert_today_intention(user.id, status="active", midday_status="almost", midday_response_at=now())
        log_event(user.id, "loop_status", {"phase": "midday", "status": "almost"})
        record_intervention_outcome(user.id, trigger_type="midday_check", mode="focus", responded=True, session_started=False, progress_occurred=False, issue_repeated=False)
        await query.edit_message_text(
            render_intervention_text(user.id, "inactivity_after_target"),
            reply_markup=focus_duration_buttons(),
        )
        return

    if data == "loop:midday:avoiding":
        upsert_today_intention(user.id, midday_status="avoiding", midday_response_at=now())
        log_event(user.id, "loop_status", {"phase": "midday", "status": "avoiding"})
        increment_memory_counter(user.id, "time_of_day_slumps", str(local_now_for_user(user.id).hour), 1, 0.7)
        record_intervention_outcome(user.id, trigger_type="midday_check", mode="recovery", responded=True, session_started=False, progress_occurred=False, issue_repeated=True)
        await query.edit_message_text(
            "Name the blocker so I can give you the right restart.",
            reply_markup=blocker_choice_buttons("recover"),
        )
        return

    if data.startswith("recover:blocker:"):
        blocker = data.split(":")[2]
        upsert_today_intention(user.id, last_blocker=blocker)
        increment_memory_counter(user.id, "goal_friction_patterns", blocker, 1, 0.75)
        record_intervention_outcome(user.id, trigger_type="recovery_choice", mode="recovery", blocker=blocker, responded=True, session_started=False, progress_occurred=False, issue_repeated=True)
        maybe_log_goal_decay(user.id)
        await safe_edit_message_text(
            query,
            render_intervention_text(user.id, "repeated_avoidance", blocker=blocker),
            reply_markup=intervention_reply_markup(user.id, "repeated_avoidance", blocker=blocker),
        )
        return

    if data.startswith("loop:eod:"):
        status = data.split(":")[2]
        mapped = {"done": "done", "partial": "partial", "missed": "missed", "reset": "reset_tomorrow"}[status]
        upsert_today_intention(user.id, status=mapped, eod_status=mapped, eod_response_at=now())
        log_event(user.id, "loop_status", {"phase": "eod", "status": mapped})
        if status == "done":
            bump_streak(user.id, 1)
            record_intervention_outcome(user.id, trigger_type="eod_check", mode="momentum", responded=True, session_started=False, progress_occurred=True, issue_repeated=False)
            await query.edit_message_text("Logged done. Bank the win and protect tomorrow.")
        elif status == "missed":
            bump_missed(user.id, 1)
            increment_memory_counter(user.id, "time_of_day_slumps", str(local_now_for_user(user.id).hour), 1, 0.75)
            record_intervention_outcome(user.id, trigger_type="eod_check", mode="recovery", responded=True, session_started=False, progress_occurred=False, issue_repeated=True)
            await safe_edit_message_text(
                query,
                render_intervention_text(user.id, "missed_day"),
                reply_markup=intervention_reply_markup(user.id, "missed_day"),
            )
        elif status == "reset":
            record_intervention_outcome(user.id, trigger_type="eod_check", mode="starter", responded=True, session_started=False, progress_occurred=False, issue_repeated=False)
            await query.edit_message_text("Reset accepted. Tomorrow starts with a clean board.")
        else:
            record_intervention_outcome(user.id, trigger_type="eod_check", mode="momentum", responded=True, session_started=False, progress_occurred=True, issue_repeated=False)
            await query.edit_message_text("Partial logged. Keep the useful residue and come back tomorrow.")
        return

    # Mood selected
    if data.startswith("mood:"):
        mood = data.split(":")[1]
        state.update_one({"user_id": user.id}, {"$set": {"mood": mood}}, upsert=True)
        g = resolve_current_goal(user.id)
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
        goal_ref = data.split(":")[1]
        goal_doc = get_goal_by_ref(user.id, goal_ref)
        if goal_doc and set_active_goal(user.id, goal_ref):
            upsert_today_intention(user.id, selected_goal=goal_doc["goal"])
            await query.edit_message_text(f"Active goal set to: {goal_doc['goal']}")
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
    ensure_user(user.id, user.full_name or user.username or "human")
    touch_user(user.id, "text")
    txt = (update.message.text or "").strip()
    logger.info("text_router received message from user_id=%s text=%r", user.id if user else None, txt)

    conversation = get_conversation(user.id)
    if conversation:
        kind = conversation.get("kind")
        step = conversation.get("step")
        data = conversation.get("data") or {}

        if kind == "onboarding" and step == "timezone_text":
            try:
                ZoneInfo(txt)
            except Exception:
                return await update.message.reply_text("That timezone didn't validate. Send an IANA timezone like `America/Toronto`.", parse_mode="Markdown")
            set_profile_fields(user.id, timezone=txt)
            users.update_one({"user_id": user.id}, {"$set": {"tz": txt}}, upsert=True)
            set_profile_conversation(user.id, "onboarding", "goal_name", {"goal_count": int(data.get("goal_count", 0))})
            return await update.message.reply_text("Nice. Now send goal 1 in a few words.")

        if kind == "onboarding" and step == "goal_name":
            goal_name = _slugify_goal(txt)
            goal_count = int(data.get("goal_count", goals.count_documents({"user_id": user.id})))
            set_profile_conversation(user.id, "onboarding", "goal_why", {"goal_count": goal_count, "goal_name": goal_name})
            return await update.message.reply_text(f"Why does **{goal_name}** matter to you?", parse_mode="Markdown")

        if kind == "onboarding" and step == "goal_why":
            goal_name = data.get("goal_name")
            goal_count = int(data.get("goal_count", goals.count_documents({"user_id": user.id})))
            set_goal_why(user.id, goal_name, txt)
            if not (users.find_one({"user_id": user.id}) or {}).get("active_goal"):
                users.update_one({"user_id": user.id}, {"$set": {"active_goal": goal_name}}, upsert=True)
            goal_count += 1
            set_profile_conversation(user.id, "onboarding", "goal_more", {"goal_count": goal_count})
            return await update.message.reply_text(
                f"Saved goal {goal_count}: **{goal_name}**.\nAdd another active goal or continue setup.",
                parse_mode="Markdown",
                reply_markup=goal_more_buttons(goal_count),
            )

        if kind == "intention" and step == "target_text":
            goal = data.get("selected_goal")
            upsert_today_intention(user.id, selected_goal=goal, target=txt, status="planned")
            set_profile_conversation(user.id, "intention", "fallback_text", {"selected_goal": goal, "target": txt})
            return await update.message.reply_text(f"If **{goal}** goes sideways, what's your fallback?", parse_mode="Markdown")

        if kind == "intention" and step == "fallback_text":
            goal = data.get("selected_goal")
            target = data.get("target")
            upsert_today_intention(user.id, selected_goal=goal, target=target, fallback=txt, status="active")
            increment_memory_counter(user.id, "goal_friction_patterns", goal, 1, 0.55)
            clear_profile_conversation(user.id)
            intention = get_today_intention(user.id) or {}
            return await update.message.reply_text(
                intention_summary(user.id),
                reply_markup=intention_action_buttons(intention.get("status")),
            )

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

    g = resolve_current_goal(user.id)
    goal = g["goal"] if g else "—"
    prompt = (
        f"User said: '{txt}'.\n"
        f"Current focus goal: '{goal}'.\n"
        "Reply as a concise, no-nonsense accountability coach. Offer a smallest next step."
    )
    reply = ai_reply(prompt)
    await update.message.reply_text(reply)

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("Telegram handler error", exc_info=context.error)

async def run_override(user_id: int, goal: str, context: ContextTypes.DEFAULT_TYPE):
    step1 = "Grounding: 6 cycles — inhale 4, hold 4, exhale 6. Drink water. Stand up and shake arms."
    why = get_why(user_id, goal) or "—"
    step2 = f"Your why: “{why}”."
    step3 = f"Smallest action: open the tool. If {goal == 'code'} → open VS Code; if gym → put on shoes. 90-second rule."
    log_event(user_id, "override", {"goal": goal})
    await context.bot.send_message(chat_id=user_id, text=f"{step1}\n\n{step2}\n\n{step3}")

def loop_hours_for_user(user_id: int) -> Dict[str, int]:
    profile = ensure_profile(user_id)
    work_start = int(profile.get("work_start_hour", 9))
    return {
        "morning": max(6, work_start - 1),
        "midday": min(15, work_start + 4),
        "eod": min(22, work_start + 10),
    }

async def send_intervention_message(app: Application, user_id: int, trigger: str, *, blocker: str | None = None, session_doc: Dict[str, Any] | None = None, reply_markup=None):
    intervention = choose_intervention(user_id, trigger, blocker=blocker, session_doc=session_doc)
    text = phrase_intervention(user_id, intervention)
    await app.bot.send_message(chat_id=user_id, text=text, reply_markup=reply_markup or premium_action_buttons(user_id, intervention))
    log_structured("intervention_send", user_id=user_id, trigger=trigger, mode=intervention.get("mode"), blocker=intervention.get("blocker"), session_id=str(session_doc["_id"]) if session_doc else None)
    log_event(user_id, "intervention", {"trigger": trigger, "mode": intervention.get("mode"), "blocker": blocker, "session_id": str(session_doc["_id"]) if session_doc else None})
    record_intervention_outcome(
        user_id,
        trigger_type=trigger,
        mode=intervention.get("mode", "starter"),
        blocker=intervention.get("blocker"),
        responded=False,
        session_started=False,
        progress_occurred=False,
        issue_repeated=trigger in {"repeated_avoidance", "missed_day"},
    )

async def run_daily_loop_service(app: Application):
    now_utc = dt.datetime.now(dt.timezone.utc)
    for u in users.find({}):
        uid = u["user_id"]
        try:
            ensure_profile(uid, u.get("name", "human"))
            local_now = now_utc.astimezone(ZoneInfo(get_user_timezone(uid)))
            hour = local_now.hour
            hours = loop_hours_for_user(uid)
            intention = get_today_intention(uid) or {}
            yesterday = get_intention_for_date(uid, date_key_for_user(uid, -1)) or {}
            state_doc = get_state(uid)
            current_goal = resolve_current_goal(uid)
            last_touch = ensure_aware(state_doc.get("last_user_touch_at"))

            if yesterday.get("status") == "missed" and hour == hours["morning"] and not intention.get("missed_day_recovery_sent_at"):
                upsert_today_intention(uid, missed_day_recovery_sent_at=now(), morning_prompt_sent_at=intention.get("morning_prompt_sent_at") or now())
                await send_intervention_message(app, uid, "missed_day", reply_markup=intervention_reply_markup(uid, "missed_day"))
                continue

            if hour == hours["morning"] and not intention.get("morning_prompt_sent_at"):
                upsert_today_intention(uid, morning_prompt_sent_at=now(), status=intention.get("status") or "planned")
                await app.bot.send_message(uid, text=morning_summary_text(uid), reply_markup=morning_anchor_buttons())
                log_structured("morning_prompt_sent", user_id=uid, hour=hour, date=today_key_for_user(uid))
                log_event(uid, "daily_loop", {"phase": "morning_anchor"})
                continue

            morning_sent_at = ensure_aware(intention.get("morning_prompt_sent_at"))
            if morning_sent_at and not intention.get("morning_response_at") and now() >= morning_sent_at + timedelta(hours=2):
                if not last_touch or last_touch <= morning_sent_at:
                    if not intention.get("morning_followup_sent_at"):
                        upsert_today_intention(uid, morning_followup_sent_at=now())
                        await send_intervention_message(app, uid, "no_response_after_morning_prompt", reply_markup=morning_anchor_buttons())
                        continue

            if hour == hours["midday"] and intention.get("target") and not intention.get("midday_prompt_sent_at"):
                upsert_today_intention(uid, midday_prompt_sent_at=now())
                await app.bot.send_message(uid, text="Midday check. Where are you at?", reply_markup=midday_check_buttons())
                log_structured("midday_prompt_sent", user_id=uid, hour=hour, goal=intention.get("selected_goal"))
                log_event(uid, "daily_loop", {"phase": "midday"})
                continue

            target_updated_at = ensure_aware(intention.get("updated_at"))
            if intention.get("target") and intention.get("status") in {"planned", "active", "partial", "blocked"} and not get_active_session(uid):
                if target_updated_at and now() >= target_updated_at + timedelta(minutes=90):
                    if not intention.get("target_inactivity_sent_at") and (not last_touch or last_touch <= target_updated_at):
                        upsert_today_intention(uid, target_inactivity_sent_at=now())
                        await send_intervention_message(app, uid, "inactivity_after_target", reply_markup=focus_duration_buttons())
                        continue

            if hour == hours["eod"] and intention.get("target") and not intention.get("eod_prompt_sent_at"):
                upsert_today_intention(uid, eod_prompt_sent_at=now())
                await app.bot.send_message(uid, text="End of day check. How did it go?", reply_markup=end_of_day_buttons())
                log_structured("eod_prompt_sent", user_id=uid, hour=hour, goal=intention.get("selected_goal"))
                log_event(uid, "daily_loop", {"phase": "eod"})
                continue

            if recent_avoidance_count(uid) >= 2 and not intention.get("avoidance_recovery_sent_at"):
                upsert_today_intention(uid, avoidance_recovery_sent_at=now())
                await send_intervention_message(app, uid, "repeated_avoidance", reply_markup=intervention_reply_markup(uid, "repeated_avoidance"))
                continue

            goal_updated_at = ensure_aware((current_goal or {}).get("updated_at"))
            if current_goal and goal_updated_at and now() >= goal_updated_at + timedelta(days=7):
                if not intention and not state_doc.get("stale_goal_sent_at"):
                    state.update_one({"user_id": uid}, {"$set": {"stale_goal_sent_at": now()}}, upsert=True)
                    maybe_log_goal_decay(uid, current_goal.get("goal"))
                    await send_intervention_message(
                        app,
                        uid,
                        "goal_decay" if detect_goal_decay(uid, current_goal.get("goal")).get("decayed") else "stale_goal",
                        reply_markup=intervention_reply_markup(uid, "goal_decay" if detect_goal_decay(uid, current_goal.get("goal")).get("decayed") else "stale_goal"),
                    )
        except Exception:
            logger.exception("Daily loop service failed for user_id=%s", uid)

# =========================
# CRON TASKS (hit by Cloudflare Cron)
# =========================

def _hour_bucket(dt_utc): return dt_utc.strftime("%Y-%m-%dT%H")

async def cron_daily(app: Application):
    """Run the daily loop prompts and recovery checks."""
    log_structured("cron_daily_start")
    await run_daily_loop_service(app)
    log_structured("cron_daily_finish")

async def cron_weekly(app: Application):
    """Send a deterministic weekly summary phrased by AI."""
    log_structured("cron_weekly_start")
    for u in users.find({}):
        uid = u["user_id"]
        try:
            facts = weekly_summary_facts(uid)
            msg = phrase_weekly_summary(uid, facts)
            await app.bot.send_message(chat_id=uid, text=msg)
            log_structured("weekly_summary_sent", user_id=uid, days_active=facts.get("days_active"), main_blocker=facts.get("main_blocker_pattern"), what_worked=facts.get("what_worked"))
            log_event(uid, "insight", facts)
            set_memory(uid, "last_weekly_summary", facts, 0.85)
        except Exception:
            logger.exception("Weekly summary failed for user_id=%s", uid)
    log_structured("cron_weekly_finish")

def ops_summary_payload(hours: int = 24) -> Dict[str, Any]:
    cutoff = recent_cutoff(hours)
    daily_loop_logs = list(logs.find({"kind": "daily_loop", "ts": {"$gte": cutoff}}))
    intervention_logs = list(logs.find({"kind": "intervention", "ts": {"$gte": cutoff}}))
    loop_status_logs = list(logs.find({"kind": "loop_status", "ts": {"$gte": cutoff}}))
    focus_logs = list(logs.find({"kind": "focus_completion", "ts": {"$gte": cutoff}}))
    session_finishes = list(logs.find({"kind": "session_finish", "ts": {"$gte": cutoff}}))
    outcomes = list(intervention_outcomes.find({"ts": {"$gte": cutoff}}))

    daily_loop_phase_counts: Dict[str, int] = {}
    for item in daily_loop_logs:
        phase = (item.get("data") or {}).get("phase")
        if phase:
            daily_loop_phase_counts[phase] = daily_loop_phase_counts.get(phase, 0) + 1

    loop_status_counts: Dict[str, int] = {}
    for item in loop_status_logs:
        status = (item.get("data") or {}).get("status")
        if status:
            loop_status_counts[status] = loop_status_counts.get(status, 0) + 1

    focus_completion_counts: Dict[str, int] = {}
    for item in focus_logs:
        status = (item.get("data") or {}).get("status")
        if status:
            focus_completion_counts[status] = focus_completion_counts.get(status, 0) + 1

    onboarding_dropoff = count_docs(
        profiles,
        {
            "onboarding_complete": False,
            "created_at": {"$lte": now() - timedelta(hours=24)},
        },
    )

    return {
        "window_hours": hours,
        "timezone_default": TZ,
        "prompt_delivery": {
            "daily_loop": daily_loop_phase_counts,
            "intervention_sends": len(intervention_logs),
        },
        "user_responses": {
            "loop_status": loop_status_counts,
            "focus_completion": focus_completion_counts,
        },
        "intervention_outcomes": {
            "total": len(outcomes),
            "responded": sum(1 for item in outcomes if item.get("responded")),
            "session_started": sum(1 for item in outcomes if item.get("session_started")),
            "progress_occurred": sum(1 for item in outcomes if item.get("progress_occurred")),
            "issue_repeated": sum(1 for item in outcomes if item.get("issue_repeated")),
        },
        "onboarding": {
            "incomplete_profiles": count_docs(profiles, {"onboarding_complete": False}),
            "dropoff_24h": onboarding_dropoff,
        },
        "sessions": {
            "active": count_docs(sessions, {"state": "ACTIVE"}),
            "finishes": aggregate_status_counts([item.get("data") or {} for item in session_finishes], "to"),
        },
    }

# =========================
# FASTAPI WIRING
# =========================
app = FastAPI(title="Brobot v2 (webhook)")

# Build Telegram application once
tg_app: Application = ApplicationBuilder().token(BOT_TOKEN).concurrent_updates(True).build()

# Handlers
tg_app.add_handler(CommandHandler("start", cmd_start))
tg_app.add_handler(CommandHandler("settings", cmd_settings))
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
tg_app.add_error_handler(on_error)

@app.on_event("startup")
async def verify_dependencies():
    await tg_app.initialize()
    await tg_app.start()
    try:
        mongo.admin.command("ping")
        logger.info("Mongo ok")
    except PyMongoError as e:
        raise RuntimeError(f"Mongo ping failed: {e}")

    webhook_base = (WEBHOOK_URL or RENDER_EXTERNAL_URL or "").rstrip("/")
    if webhook_base:
        webhook_url = f"{webhook_base}/webhook"
        try:
            await tg_app.bot.set_webhook(
                url=webhook_url,
                secret_token=TELEGRAM_SECRET_TOKEN or None,
                allowed_updates=Update.ALL_TYPES,
            )
            logger.info("Webhook set to %s", webhook_url)
        except Exception:
            logger.exception("Failed to set webhook to %s", webhook_url)
            raise
    else:
        logger.warning("WEBHOOK_URL/RENDER_EXTERNAL_URL not set; webhook was not auto-registered")
    
@app.on_event("shutdown")
async def on_shutdown():
    await tg_app.stop()
    await tg_app.shutdown()   
    
@app.get("/health")
async def health():
    return PlainTextResponse("ok")

@app.get("/ops/summary")
async def ops_summary(request: Request):
    _check_cron_auth(request)
    hours = int(request.query_params.get("hours", "24"))
    hours = max(1, min(hours, 168))
    return JSONResponse(ops_summary_payload(hours))

@app.get("/ops/verify")
async def ops_verify(request: Request):
    _check_cron_auth(request)
    expected_base = (WEBHOOK_URL or RENDER_EXTERNAL_URL or "").rstrip("/")
    mongo_ok = False
    webhook_info: Dict[str, Any] = {}
    try:
        mongo.admin.command("ping")
        mongo_ok = True
    except Exception:
        logger.exception("Mongo verification failed")
    try:
        info = await tg_app.bot.get_webhook_info()
        webhook_info = {
            "url": getattr(info, "url", ""),
            "pending_update_count": getattr(info, "pending_update_count", None),
            "last_error_date": getattr(info, "last_error_date", None),
            "last_error_message": getattr(info, "last_error_message", None),
        }
    except Exception:
        logger.exception("Webhook verification failed")
    return JSONResponse({
        "mongo_ok": mongo_ok,
        "expected_webhook_url": f"{expected_base}/webhook" if expected_base else "",
        "webhook": webhook_info,
        "cron_secret_configured": bool(CRON_SECRET),
        "timezone_default": TZ,
        "ops_summary_24h": ops_summary_payload(24),
    })

@app.post("/webhook")
async def telegram_webhook(request: Request):
    # Validate Telegram secret token if provided
    if TELEGRAM_SECRET_TOKEN:
        hdr = request.headers.get("x-telegram-bot-api-secret-token")
        if hdr != TELEGRAM_SECRET_TOKEN:
            raise HTTPException(status_code=401, detail="Invalid telegram secret token")
    data = await request.json()
    update = Update.de_json(data=data, bot=tg_app.bot)
    try:
        await tg_app.process_update(update)
    except Exception:
        logger.exception("Failed to process Telegram update")
        raise HTTPException(status_code=500, detail="Update processing failed")
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
    log_structured("cron_sessions_tick_start")
    now_utc = now()
    active = list(sessions.find({"state": "ACTIVE"}))
    for s in active:
        uid = s["user_id"]
        ends_at = ensure_aware(s.get("ends_at")) or now_utc
        if now_utc >= ends_at and not s.get("asked_completion", False):
            try:
                await app.bot.send_message(
                    uid,
                    text=f"Time's up for {_session_msg_goal_line(s)}. How did it go?",
                    reply_markup=focus_completion_buttons(),
                    parse_mode="Markdown",
                )
                log_structured("session_completion_prompt_sent", user_id=uid, session_id=str(s["_id"]), goal=s.get("goal"))
                sessions.update_one({"_id": s["_id"]}, {"$set": {"asked_completion": True}})
            except Exception:
                logger.exception("Session completion prompt failed for user_id=%s", uid)
            continue

        nca = ensure_aware(s.get("next_check_at"))
        if not s.get("nudges_enabled", True) or not nca or now_utc < nca:
            continue

        nudges = int(s.get("nudges_sent", 0))
        started = bool(s.get("started_confirmed", False))

        if not started:
            txt = f"🔥 {_session_msg_goal_line(s)} — Did you start?"
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("Yes, I started", callback_data="sess:start_yes"),
                 InlineKeyboardButton("Not yet", callback_data="sess:start_no")]
            ])
            next_dt = now_utc + timedelta(minutes=5)
        else:
            txt = f"⚡ {_session_msg_goal_line(s)} — Still in the pocket?"
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("Still working", callback_data="sess:still_yes"),
                 InlineKeyboardButton("I drifted", callback_data="sess:still_no")]
            ])
            next_dt = now_utc + timedelta(minutes=15)

        if nudges >= 4 and started:
            sessions.update_one({"_id": s["_id"]}, {"$set": {"next_check_at": next_dt}})
            continue

        try:
            await app.bot.send_message(uid, text=txt, reply_markup=kb, parse_mode="Markdown")
            log_structured("session_nudge_sent", user_id=uid, session_id=str(s["_id"]), started=started, nudges_sent=nudges + 1)
            sessions.update_one({"_id": s["_id"]}, {"$set": {"next_check_at": next_dt}, "$inc": {"nudges_sent": 1}})
        except Exception:
            logger.exception("Session tick failed for user_id=%s", uid)
    log_structured("cron_sessions_tick_finish", active_sessions=len(active))

# Endpoint to trigger it (like your other cron endpoints)
@app.get("/cron/sessions-tick")
async def cron_sessions_tick_endpoint(request: Request):
    _check_cron_auth(request)
    await cron_sessions_tick(tg_app)
    return PlainTextResponse("sessions-tick-ok")
