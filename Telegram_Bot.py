# main.py
import os
import random
import asyncio
import datetime as dt
import logging
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Any
from pymongo.errors import PyMongoError
from bson import ObjectId


from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.encoders import jsonable_encoder

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
control_stats = db["control_stats"]  # { user_id, category, bucket, attempts, successes, weighted_score, last_used_at, last_success_at, updated_at }
control_events = db["control_events"]  # { user_id, ts, outcome_type, message_type, trigger, phase, hour_bin, time_bucket, intervention_key, pressure_level, silence_reason, related_goal_id, related_session_id, updated_at }
system_state = db["system_state"]  # { _id, fake_utc_now, updated_at }
test_outbox = db["test_outbox"]  # { user_id, ts, text, message_type, phase, trigger, related_session_id, updated_at }

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
control_stats.create_index([("user_id", ASCENDING), ("category", ASCENDING), ("bucket", ASCENDING)], unique=True)
control_events.create_index([("user_id", ASCENDING), ("ts", DESCENDING)])
system_state.create_index([("_id", ASCENDING)])
test_outbox.create_index([("user_id", ASCENDING), ("ts", DESCENDING)])

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
CONTROL_TIME_BUCKETS = {
    "morning": range(6, 10),
    "late_morning": range(10, 13),
    "afternoon": range(13, 18),
    "evening": range(18, 23),
}
PRESSURE_LEVELS = ["low", "medium", "high", "sharp"]

# =========================
# UTIL
# =========================
def parse_iso_dt(value: str) -> dt.datetime:
    raw = (value or "").strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    parsed = dt.datetime.fromisoformat(raw)
    return ensure_aware(parsed) or parsed.replace(tzinfo=dt.timezone.utc)

def current_utc_now() -> dt.datetime:
    fake = system_state.find_one({"_id": "clock"}) or {}
    fake_value = fake.get("fake_utc_now")
    fake_utc = None
    if isinstance(fake_value, str):
        fake_utc = parse_iso_dt(fake_value)
    else:
        fake_utc = ensure_aware(fake_value)
    if fake_utc:
        return fake_utc.astimezone(dt.timezone.utc)
    return dt.datetime.now(dt.timezone.utc)

def now():
    return current_utc_now().astimezone(TZINFO)

def ensure_aware(ts: dt.datetime | None) -> dt.datetime | None:
    if ts is None:
        return None
    if ts.tzinfo is None:
        # PyMongo returns naive UTC datetimes by default.
        return ts.replace(tzinfo=dt.timezone.utc)
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
    return current_utc_now().astimezone(ZoneInfo(get_user_timezone(user_id)))

def today_key_for_user(user_id: int) -> str:
    return local_now_for_user(user_id).date().isoformat()

def date_key_for_user(user_id: int, delta_days: int = 0) -> str:
    return (local_now_for_user(user_id).date() + timedelta(days=delta_days)).isoformat()

def active_goal_query(user_id: int) -> Dict[str, Any]:
    return {
        "user_id": user_id,
        "$or": [
            {"status": {"$exists": False}},
            {"status": {"$in": ["active", "ACTIVE"]}},
        ],
    }

def list_user_goals(user_id: int, *, include_completed: bool = False):
    query = {"user_id": user_id} if include_completed else active_goal_query(user_id)
    return list(goals.find(query).sort([("updated_at", DESCENDING), ("goal", ASCENDING)]))

def get_goal_by_ref(user_id: int, goal_ref: str, *, include_completed: bool = False):
    base_query = {"user_id": user_id} if include_completed else active_goal_query(user_id)
    try:
        query = dict(base_query)
        query["_id"] = ObjectId(goal_ref)
        return goals.find_one(query)
    except Exception:
        query = dict(base_query)
        query["goal"] = goal_ref
        return goals.find_one(query)

def get_goal_by_name(user_id: int, goal: str, *, include_completed: bool = False):
    query = {"user_id": user_id, "goal": goal} if include_completed else {
        "user_id": user_id,
        "goal": goal,
        "$or": [
            {"status": {"$exists": False}},
            {"status": {"$in": ["active", "ACTIVE"]}},
        ],
    }
    return goals.find_one(query)

def is_goal_active(user_id: int, goal: str | None) -> bool:
    if not goal:
        return False
    return get_goal_by_name(user_id, goal) is not None

def resolve_current_goal(user_id: int, *, sync_active: bool = True):
    user_doc = users.find_one({"user_id": user_id}) or {}
    active_goal = user_doc.get("active_goal")
    if active_goal:
        active_doc = goals.find_one({
            "user_id": user_id,
            "goal": active_goal,
            "$or": [
                {"status": {"$exists": False}},
                {"status": {"$in": ["active", "ACTIVE"]}},
            ],
        })
        if active_doc:
            return active_doc

    ordered_goals = list_user_goals(user_id)
    if not ordered_goals:
        if sync_active and active_goal:
            users.update_one({"user_id": user_id}, {"$unset": {"active_goal": ""}})
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

def mark_goal_status(user_id: int, goal: str, status: str):
    normalized = (status or "active").lower()
    updates: Dict[str, Any] = {"status": normalized, "updated_at": now()}
    if normalized == "done":
        updates["completed_at"] = now()
    else:
        updates["completed_at"] = None
    goals.update_one({"user_id": user_id, "goal": goal}, {"$set": updates}, upsert=False)
    if normalized == "done":
        next_goal_name = None
        if (users.find_one({"user_id": user_id}) or {}).get("active_goal") == goal:
            next_goal = resolve_current_goal(user_id, sync_active=False)
            if next_goal and next_goal.get("goal") != goal:
                next_goal_name = next_goal["goal"]
                users.update_one({"user_id": user_id}, {"$set": {"active_goal": next_goal["goal"]}}, upsert=True)
            else:
                users.update_one({"user_id": user_id}, {"$unset": {"active_goal": ""}})
        else:
            next_goal = resolve_current_goal(user_id, sync_active=False)
            if next_goal and next_goal.get("goal") != goal:
                next_goal_name = next_goal["goal"]
        today_intention = get_today_intention(user_id)
        if today_intention and today_intention.get("selected_goal") == goal:
            intention_updates: Dict[str, Any] = {
                "selected_goal": next_goal_name,
                "updated_at": now(),
            }
            if next_goal_name:
                intention_updates["status"] = "planned"
                intention_updates["target"] = None
                intention_updates["fallback"] = None
            daily_intentions.update_one(
                {"_id": today_intention["_id"]},
                {"$set": intention_updates},
            )

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

def hour_time_bucket(hour: int) -> str:
    for bucket, hours in CONTROL_TIME_BUCKETS.items():
        if hour in hours:
            return bucket
    return "night"

def get_control_stat(user_id: int, category: str, bucket: str) -> Dict[str, Any]:
    return control_stats.find_one({"user_id": user_id, "category": category, "bucket": bucket}) or {}

def control_stat_rank(stat: Dict[str, Any]) -> float:
    decay = _stat_recency_multiplier(stat)
    attempts = float(stat.get("attempts", 0) or 0.0) * decay
    successes = float(stat.get("successes", 0) or 0.0) * decay
    weighted = float(stat.get("weighted_score", 0.0) or 0.0) * decay
    if attempts <= 0:
        return weighted
    success_rate = successes / max(attempts, 1)
    confidence = min(attempts / 6.0, 1.0)
    return weighted + (success_rate * 0.75 * confidence)

def update_control_stat(
    user_id: int,
    category: str,
    bucket: str,
    *,
    attempts_delta: int = 0,
    successes_delta: int = 0,
    weighted_delta: float = 0.0,
    mark_used: bool = False,
    mark_success: bool = False,
):
    set_fields: Dict[str, Any] = {"updated_at": now()}
    if mark_used:
        set_fields["last_used_at"] = now()
    if mark_success:
        set_fields["last_success_at"] = now()
    inc_fields: Dict[str, Any] = {}
    if attempts_delta:
        inc_fields["attempts"] = int(attempts_delta)
    if successes_delta:
        inc_fields["successes"] = int(successes_delta)
    if weighted_delta:
        inc_fields["weighted_score"] = float(weighted_delta)
    on_insert_fields: Dict[str, Any] = {
        "user_id": user_id,
        "category": category,
        "bucket": bucket,
    }
    for field, default in {
        "attempts": 0,
        "successes": 0,
        "weighted_score": 0.0,
    }.items():
        if field not in inc_fields:
            on_insert_fields[field] = default
    update_doc: Dict[str, Any] = {
        "$set": set_fields,
        "$setOnInsert": on_insert_fields,
    }
    if inc_fields:
        update_doc["$inc"] = inc_fields
    control_stats.update_one(
        {"user_id": user_id, "category": category, "bucket": bucket},
        update_doc,
        upsert=True,
    )

def get_pending_control(user_id: int) -> Dict[str, Any] | None:
    return (get_state(user_id) or {}).get("pending_control")

def set_pending_control(user_id: int, payload: Dict[str, Any] | None):
    state.update_one({"user_id": user_id}, {"$set": {"pending_control": payload}}, upsert=True)

def clear_pending_control(user_id: int):
    state.update_one({"user_id": user_id}, {"$unset": {"pending_control": ""}}, upsert=True)

def recent_control_events(user_id: int, *, outcome_types: list[str] | None = None, hours: int = 24) -> list[Dict[str, Any]]:
    query: Dict[str, Any] = {"user_id": user_id, "ts": {"$gte": recent_cutoff(hours)}}
    if outcome_types:
        query["outcome_type"] = {"$in": outcome_types}
    return list(control_events.find(query).sort("ts", DESCENDING))

def parse_control_intervention_key(intervention_key: str | None) -> Dict[str, str]:
    parts = str(intervention_key or "").split(":")
    if len(parts) >= 5:
        return {
            "mode": parts[0],
            "blocker": parts[1],
            "pressure_level": parts[2],
            "action_offer": parts[3],
            "phrasing_style": parts[4],
        }
    return {}

def _stat_recency_multiplier(stat: Dict[str, Any], *, half_life_days: float = 14.0) -> float:
    ts = ensure_aware(stat.get("last_used_at")) or ensure_aware(stat.get("last_success_at")) or ensure_aware(stat.get("updated_at"))
    if not ts:
        return 1.0
    age_seconds = max((current_utc_now() - ts.astimezone(dt.timezone.utc)).total_seconds(), 0.0)
    age_days = age_seconds / 86400.0
    return 0.5 ** (age_days / max(half_life_days, 0.1))

def _parent_action_bucket(mode: str, blocker_name: str, action_offer: str) -> str:
    return f"{mode}:{blocker_name}:{action_offer}"

def recent_low_yield_action_patterns(user_id: int, *, hours: int = 36) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    negative_outcomes = {"message_skip", "message_skipped", "message_defer", "message_deferred", "no_response", "repeated_avoidance", "missed_day"}
    for event in recent_control_events(user_id, outcome_types=list(negative_outcomes), hours=hours):
        parent_action_key = event.get("parent_action_key")
        if parent_action_key:
            counts[str(parent_action_key)] = counts.get(str(parent_action_key), 0) + 1
            mode_name = str(parent_action_key).split(":", 1)[0]
            counts[f"mode:{mode_name}"] = counts.get(f"mode:{mode_name}", 0) + 1
            continue
        parsed = parse_control_intervention_key(event.get("intervention_key"))
        if not parsed:
            continue
        action_bucket = _parent_action_bucket(parsed["mode"], parsed["blocker"], parsed["action_offer"])
        counts[action_bucket] = counts.get(action_bucket, 0) + 1
        mode_bucket = f"mode:{parsed['mode']}"
        counts[mode_bucket] = counts.get(mode_bucket, 0) + 1
    return counts

def _stat_effective_attempts(stat: Dict[str, Any]) -> float:
    decay = _stat_recency_multiplier(stat)
    return float(stat.get("attempts", 0) or 0.0) * decay

def stat_confidence(stat: Dict[str, Any], *, target_attempts: float = 6.0, target_weighted: float = 3.0) -> float:
    decay = _stat_recency_multiplier(stat)
    attempts = float(stat.get("attempts", 0) or 0.0) * decay
    weighted = abs(float(stat.get("weighted_score", 0.0) or 0.0) * decay)
    if attempts < 1.0:
        return 0.0
    if attempts < 2.0:
        return min(attempts / 4.0, 0.35)
    attempt_conf = min(attempts / max(target_attempts, 0.1), 1.0)
    weighted_conf = min(weighted / max(target_weighted, 0.1), 1.0)
    return min((attempt_conf * 0.8) + (weighted_conf * 0.2), 1.0)

def intervention_confidence(stat: Dict[str, Any]) -> float:
    return stat_confidence(stat, target_attempts=5.0, target_weighted=3.0)

def _pressure_base_decision(user_id: int, context: Dict[str, Any] | None = None) -> tuple[str, list[str]]:
    context = context or {}
    blocker = context.get("blocker") or detect_blocker(user_id, context.get("explicit_blocker"))
    avoidance = recent_avoidance_count(user_id)
    blocked = recent_blocked_sessions(user_id)
    success = recent_success_count(user_id)
    missed = int((users.find_one({"user_id": user_id}) or {}).get("missed_days", 0))
    if blocker in {"tired", "anxious"}:
        return ("low" if missed < 3 else "medium"), ["low", "medium"]
    if blocked >= 2:
        return "medium", ["low", "medium"]
    if avoidance >= 4:
        return "sharp", ["medium", "high", "sharp"]
    if avoidance >= 2 or missed >= 2:
        return "high", ["medium", "high"]
    if success >= 3:
        return "low", ["low", "medium"]
    return "medium", ["low", "medium", "high"]

def _pressure_candidate_score(user_id: int, pressure: str, context: Dict[str, Any] | None = None) -> tuple[float, float, float]:
    context = context or {}
    message_type = context.get("message_type")
    phase = context.get("phase")
    trigger = context.get("trigger")
    stats: list[Dict[str, Any]] = [get_control_stat(user_id, "pressure_level", pressure)]
    if message_type:
        stats.append(get_control_stat(user_id, "pressure_by_message", f"{message_type}:{pressure}"))
    if phase:
        stats.append(get_control_stat(user_id, "pressure_phase", f"{phase}:{pressure}"))
    if trigger:
        stats.append(get_control_stat(user_id, "pressure_trigger", f"{trigger}:{pressure}"))
    scores = [control_stat_rank(stat) for stat in stats]
    confidences = [stat_confidence(stat, target_attempts=4.0, target_weighted=2.0) for stat in stats]
    evidence = max((_stat_effective_attempts(stat) for stat in stats), default=0.0)
    learned_score = (scores[0] * 0.45) + (scores[1] * 0.30 if len(scores) > 1 else 0.0) + (scores[2] * 0.15 if len(scores) > 2 else 0.0) + (scores[3] * 0.10 if len(scores) > 3 else 0.0)
    learned_confidence = max(confidences) if confidences else 0.0
    return learned_score, learned_confidence, evidence

def precision_reentry_state(user_id: int, context: Dict[str, Any] | None = None) -> Dict[str, Any]:
    context = context or {}
    recent_negative = recent_control_events(user_id, outcome_types=["message_skip", "message_skipped", "message_defer", "message_deferred"], hours=24)
    recent_positive = recent_control_events(user_id, outcome_types=["same_day_return", "next_day_return", "session_started", "session_completed", "progress_marked"], hours=18)
    skip_count = sum(1 for item in recent_negative if str(item.get("outcome_type")).startswith("message_skip"))
    defer_count = sum(1 for item in recent_negative if str(item.get("outcome_type")).startswith("message_defer"))
    latest_negative_ts = ensure_aware(recent_negative[0].get("ts")) if recent_negative else None
    latest_positive_ts = ensure_aware(recent_positive[0].get("ts")) if recent_positive else None
    recovered_after_silence = bool(latest_negative_ts and latest_positive_ts and latest_positive_ts >= latest_negative_ts)
    active = (
        not recovered_after_silence
        and (
            (skip_count >= 2)
            or (defer_count >= 3)
            or ((skip_count + defer_count) >= 2 and not recent_positive)
        )
    )
    return {
        "active": active,
        "skip_count": skip_count,
        "defer_count": defer_count,
        "negative_count": len(recent_negative),
        "recovered_after_silence": recovered_after_silence,
    }

def choose_pressure_level(user_id: int, context: Dict[str, Any] | None = None) -> str:
    context = context or {}
    candidate_pressures = list(context.get("candidate_pressures") or [])
    base_pressure, allowed_pressures = _pressure_base_decision(user_id, context)
    if not candidate_pressures:
        candidate_pressures = list(allowed_pressures)
    else:
        candidate_pressures = [pressure for pressure in candidate_pressures if pressure in allowed_pressures]
        if not candidate_pressures:
            candidate_pressures = list(allowed_pressures)
    ranked: list[tuple[float, str]] = []
    best_confidence = 0.0
    candidate_evidence: Dict[str, float] = {}
    pressure_order = {name: idx for idx, name in enumerate(PRESSURE_LEVELS)}
    for pressure in candidate_pressures:
        learned_score, learned_confidence, evidence = _pressure_candidate_score(user_id, pressure, context)
        best_confidence = max(best_confidence, learned_confidence)
        candidate_evidence[pressure] = evidence
        distance_penalty = abs(pressure_order[pressure] - pressure_order[base_pressure]) * 0.18
        base_bias = 0.35 if pressure == base_pressure else 0.0
        score = base_bias + (learned_score * learned_confidence) - distance_penalty
        if context.get("precision_reentry"):
            score += learned_confidence * 0.1
        ranked.append((score, pressure))
    pressure = base_pressure
    if ranked and best_confidence >= 0.6:
        ranked_map = {name: score for score, name in ranked}
        winner = max(ranked, key=lambda item: item[0])[1]
        base_score = ranked_map.get(base_pressure, -9999.0)
        winner_score = ranked_map.get(winner, -9999.0)
        if candidate_evidence.get(winner, 0.0) >= 3.0 and winner_score >= base_score + 0.08:
            pressure = winner
    push_recent_memory(user_id, "recent_pressure_levels", pressure, limit=6, confidence=0.7)
    return pressure

def _pressure_rank_boost(user_id: int, pressure: str) -> float:
    stat = get_control_stat(user_id, "pressure_level", pressure)
    return control_stat_rank(stat)

def candidate_intervention_actions(user_id: int, mode: str, trigger: str, blocker_name: str, goal: str, restart: int, decay: Dict[str, Any]) -> list[Dict[str, Any]]:
    candidates: list[Dict[str, Any]] = []
    if mode == "clarity":
        if decay.get("action") == "replace":
            candidates.append({"action_offer": "replace_goal", "action": f"{goal} is dragging too much friction. Replace it or switch goals for today.", "priority": 1.0})
        if decay.get("action") == "split":
            candidates.append({"action_offer": "split_goal", "action": f"{goal} is too heavy as one lump. Split it into a smaller visible chunk.", "priority": 0.95})
        candidates.append({"action_offer": "shrink_target", "action": f"Shrink {goal} until it feels almost too easy to start.", "priority": 0.9})
        candidates.append({"action_offer": "next_visible_win", "action": f"Pick the next visible win for {goal}. Name one outcome you can finish today and ignore the rest.", "priority": 0.8})
    elif mode == "momentum":
        candidates.append({"action_offer": "restart_block", "action": missed_day_action(user_id, goal, restart) if trigger == "missed_day" else f"Reset cleanly today. Choose a smaller target for {goal} and protect {restart} minutes for it.", "priority": 1.0})
        candidates.append({"action_offer": "shrink_target", "action": f"Cut {goal} down to one visible move and protect {restart} minutes for it.", "priority": 0.85})
    elif mode == "override":
        candidates.append({"action_offer": "override_reset", "action": f"Stop spiraling. Breathe, stand up, and do the smallest safe action toward {goal} right now.", "priority": 1.0})
        candidates.append({"action_offer": "start_5", "action": f"Do exactly 5 minutes on {goal}. Then reassess.", "priority": 0.75})
    elif mode == "recovery":
        candidates.append({"action_offer": "rescue_plan", "action": f"Reset the board. Pick one useful move on {goal}, ignore side quests, and restart for {min(restart, 10)} minutes.", "priority": 1.0})
        candidates.append({"action_offer": "start_5", "action": blocker_action("tired" if blocker_name in {"tired", "anxious"} else blocker_name, 5, goal), "priority": 0.9})
        candidates.append({"action_offer": "smallest_step", "action": blocker_action(blocker_name, restart, goal), "priority": 0.85})
    elif mode == "focus":
        candidates.append({"action_offer": "start_focus", "action": f"Protect {restart} minutes on {goal} right now. Start before you negotiate with yourself.", "priority": 1.0})
        candidates.append({"action_offer": "smallest_step", "action": blocker_action(blocker_name, restart, goal), "priority": 0.9})
        candidates.append({"action_offer": "shrink_target", "action": f"Shrink {goal} to one visible move, then start a short focus block.", "priority": 0.8})
    else:
        candidates.append({"action_offer": "smallest_step", "action": blocker_action(blocker_name, restart, goal), "priority": 1.0})
        candidates.append({"action_offer": "start_5", "action": f"Start {goal} for 5 minutes. Momentum first, quality later.", "priority": 0.9})
        candidates.append({"action_offer": "restart_block", "action": f"Choose one useful target for {goal} and protect {restart} minutes for it.", "priority": 0.8})
    return candidates

def choose_ranked_candidate(user_id: int, mode: str, blocker_name: str, pressure_level: str, candidates: list[Dict[str, Any]]) -> Dict[str, Any]:
    best = None
    best_score = -9999.0
    recent_actions = recent_list_memory(user_id, "recent_action_offers", limit=4)
    low_yield_patterns = recent_low_yield_action_patterns(user_id)
    precision_state = precision_reentry_state(user_id, {"mode": mode, "pressure_level": pressure_level})
    for candidate in candidates:
        bucket = f"{mode}:{blocker_name}:{pressure_level}:{candidate['action_offer']}"
        detail_stat = get_control_stat(user_id, "intervention", bucket)
        stat_score = control_stat_rank(detail_stat)
        parent_bucket = _parent_action_bucket(mode, blocker_name, candidate["action_offer"])
        parent_stat = get_control_stat(user_id, "intervention_parent", parent_bucket)
        parent_score = control_stat_rank(parent_stat)
        detail_conf = intervention_confidence(detail_stat)
        parent_conf = intervention_confidence(parent_stat)
        detail_influence = 0.15 + (detail_conf * 0.75)
        parent_influence = 0.1 + (parent_conf * 0.55)
        exploitation_boost = max(detail_conf, parent_conf) * (0.18 if precision_state.get("active") else 0.08)
        repetition_penalty = 0.0
        if recent_actions:
            if candidate["action_offer"] == recent_actions[0]:
                repetition_penalty = 0.45
            elif candidate["action_offer"] in recent_actions[:2]:
                repetition_penalty = 0.25
            elif candidate["action_offer"] in recent_actions:
                repetition_penalty = 0.12
        low_yield_penalty = min(low_yield_patterns.get(parent_bucket, 0) * 0.35, 1.05)
        low_yield_penalty += min(low_yield_patterns.get(f"mode:{mode}", 0) * 0.08, 0.24)
        precision_bonus = 0.0
        precision_block_penalty = 0.0
        if precision_state.get("active") and low_yield_patterns.get(parent_bucket, 0) >= 2:
            precision_block_penalty = min(0.45 + ((low_yield_patterns.get(parent_bucket, 0) - 2) * 0.2), 0.85)
        if precision_state.get("active") and candidate["action_offer"] in {"start_5", "smallest_step", "rescue_plan"} and precision_block_penalty == 0.0:
            precision_bonus = 0.18
        score = (
            float(candidate.get("priority", 0.0))
            + (stat_score * detail_influence)
            + (parent_score * parent_influence)
            + (_pressure_rank_boost(user_id, pressure_level) * 0.1)
            + exploitation_boost
            + precision_bonus
            - repetition_penalty
            - low_yield_penalty
            - precision_block_penalty
        )
        if score > best_score:
            best = candidate
            best_score = score
    return best or candidates[0]

def choose_ranked_phrasing_style(user_id: int, mode: str, tone_policy: str, pressure_level: str, *, precision_reentry: bool = False) -> str:
    candidates = list(MODE_STYLE_CANDIDATES.get(mode, ["tactical", "compressed"]))
    if precision_reentry:
        candidates = ["compressed", "blunt", "tactical"] + [c for c in candidates if c not in {"compressed", "blunt", "tactical"}]
    if tone_policy == "confrontational":
        candidates = ["confrontational", "blunt"] + [c for c in candidates if c not in {"confrontational", "blunt"}]
    elif tone_policy == "calm":
        candidates = ["calm", "tactical"] + [c for c in candidates if c not in {"calm", "tactical"}]
    elif tone_policy == "compressed":
        candidates = ["compressed", "blunt"] + [c for c in candidates if c not in {"compressed", "blunt"}]
    recent_styles = recent_list_memory(user_id, "recent_phrasing_styles", limit=3)
    best_style = candidates[0]
    best_score = -9999.0
    for idx, style in enumerate(candidates):
        bucket = f"{mode}:{pressure_level}:{style}"
        stat_score = control_stat_rank(get_control_stat(user_id, "phrasing_style", bucket))
        repetition_penalty = 0.35 if style in recent_styles[:2] else 0.0
        precision_bonus = 0.2 if precision_reentry and style == "compressed" else 0.0
        score = (1.0 - (idx * 0.08)) + stat_score + precision_bonus - repetition_penalty
        if score > best_score:
            best_style = style
            best_score = score
    return best_style

def choose_best_time_window(user_id: int, context: Dict[str, Any]) -> int:
    phase = str(context.get("phase") or "general")
    default_hour = int(context.get("default_hour", 9))
    if phase == "morning":
        offsets = [0, 1, -1, 2]
    elif phase == "midday":
        offsets = [0, -1, 1, 2]
    else:
        offsets = [0, -1, 1, 2]
    candidates: list[int] = []
    for offset in offsets:
        hour = max(6, min(23, default_hour + offset))
        if hour not in candidates:
            candidates.append(hour)
    total_attempts = 0
    best_hour = default_hour
    best_score = -9999.0
    activity = get_memory(user_id, "time_of_day_activity", {}) or {}
    slumps = get_memory(user_id, "time_of_day_slumps", {}) or {}
    for hour in candidates:
        stat = get_control_stat(user_id, "timing_hour", f"{phase}:{hour}")
        total_attempts += float(stat.get("attempts", 0) or 0.0) * _stat_recency_multiplier(stat)
        stat_score = control_stat_rank(stat)
        stat_conf = stat_confidence(stat, target_attempts=4.0, target_weighted=2.0)
        activity_score = float(activity.get(str(hour), 0)) * 0.04
        slump_penalty = float(slumps.get(str(hour), 0)) * 0.08
        proximity_bonus = (0.02 if context.get("precision_reentry") else 0.1) if hour == default_hour else 0.0
        precision_bonus = (stat_score * stat_conf * 0.25) if context.get("precision_reentry") else 0.0
        score = stat_score + activity_score - slump_penalty + proximity_bonus + precision_bonus
        if score > best_score:
            best_hour = hour
            best_score = score
    return best_hour if total_attempts >= 3 else default_hour

def should_send_message(user_id: int, message_type: str, context: Dict[str, Any] | None = None) -> Dict[str, Any]:
    context = context or {}
    state_doc = get_state(user_id)
    recent_sent = recent_control_events(user_id, outcome_types=["proactive_sent"], hours=6)
    recent_positive = recent_control_events(user_id, outcome_types=["same_day_return", "next_day_return", "session_started", "session_completed", "progress_marked"], hours=6)
    pending = state_doc.get("pending_control") or {}
    pending_sent_at = ensure_aware(pending.get("sent_at"))
    if get_active_session(user_id) and message_type not in {"session_nudge", "session_completion"}:
        return {"decision": "defer", "reason": "active_session"}
    if cooldown_active(user_id) and message_type in {"intervention", "morning_followup"}:
        return {"decision": "defer", "reason": "cooldown"}
    if pending_sent_at and not pending.get("resolved") and now() < pending_sent_at + timedelta(minutes=90) and message_type in {"morning_followup", "intervention", "midday_prompt", "eod_prompt"}:
        return {"decision": "defer", "reason": "recent_unanswered_prompt"}
    if len(recent_sent) >= 3 and not recent_positive and message_type not in {"session_completion"}:
        return {"decision": "skip", "reason": "low_yield_burst"}
    if context.get("pressure_level") == "low" and detect_blocker(user_id) in {"tired", "anxious"} and len(recent_sent) >= 2:
        return {"decision": "defer", "reason": "overload_backoff"}
    return {"decision": "send", "reason": "allowed"}

def record_outcome(user_id: int, event: Dict[str, Any]) -> Dict[str, Any]:
    ts = event.get("ts") or now()
    local_ts = ensure_aware(ts).astimezone(ZoneInfo(get_user_timezone(user_id)))
    pending = get_pending_control(user_id) or {}
    doc = {
        "user_id": user_id,
        "ts": ts,
        "updated_at": now(),
        "outcome_type": event.get("outcome_type"),
        "message_type": event.get("message_type") or pending.get("message_type"),
        "trigger": event.get("trigger") or pending.get("trigger"),
        "phase": event.get("phase") or pending.get("phase"),
        "hour_bin": event.get("hour_bin", pending.get("hour_bin", local_ts.hour)),
        "time_bucket": event.get("time_bucket") or pending.get("time_bucket") or hour_time_bucket(local_ts.hour),
        "intervention_key": event.get("intervention_key") or pending.get("intervention_key"),
        "pressure_level": event.get("pressure_level") or pending.get("pressure_level"),
        "action_offer": event.get("action_offer") or pending.get("action_offer"),
        "phrasing_style": event.get("phrasing_style") or pending.get("phrasing_style"),
        "parent_action_key": event.get("parent_action_key") or pending.get("parent_action_key"),
        "precision_reentry": bool(event.get("precision_reentry", pending.get("precision_reentry", False))),
        "silence_reason": event.get("silence_reason"),
        "related_goal_id": event.get("related_goal_id") or event.get("goal") or effective_intention_goal(user_id),
        "related_session_id": event.get("related_session_id"),
        "responded": bool(event.get("responded", False)),
        "progress_occurred": bool(event.get("progress_occurred", False)),
        "session_started": bool(event.get("session_started", False)),
        "session_completed": bool(event.get("session_completed", False)),
        "issue_repeated": bool(event.get("issue_repeated", False)),
    }
    control_events.insert_one(doc)
    update_control_scores(user_id, doc)
    return doc

def update_control_scores(user_id: int, event: Dict[str, Any]):
    outcome = str(event.get("outcome_type") or "")
    phase = event.get("phase")
    hour_bin = event.get("hour_bin")
    time_bucket = event.get("time_bucket")
    intervention_key = event.get("intervention_key")
    pressure_level = event.get("pressure_level")
    silence_reason = event.get("silence_reason")
    score_map = {
        "proactive_sent": 0.0,
        "user_response": 0.2,
        "button_tap": 0.1,
        "same_day_return": 0.9,
        "next_day_return": 1.1,
        "session_started": 2.0,
        "session_completed": 3.0,
        "progress_marked": 2.6,
        "no_response": -0.9,
        "repeated_avoidance": -1.2,
        "missed_day": -1.4,
        "message_defer": -0.2,
        "message_deferred": -0.2,
        "message_skip": -0.3,
        "message_skipped": -0.3,
    }
    attempts_delta = 1 if outcome == "proactive_sent" else 0
    successes_delta = 1 if outcome in {"same_day_return", "next_day_return", "session_started", "session_completed", "progress_marked"} else 0
    weighted_delta = score_map.get(outcome, 0.0)
    if phase and hour_bin is not None:
        update_control_stat(user_id, "timing_hour", f"{phase}:{hour_bin}", attempts_delta=attempts_delta, successes_delta=successes_delta, weighted_delta=weighted_delta, mark_used=True, mark_success=successes_delta > 0)
    if phase and time_bucket:
        update_control_stat(user_id, "timing_bucket", f"{phase}:{time_bucket}", attempts_delta=attempts_delta, successes_delta=successes_delta, weighted_delta=weighted_delta, mark_used=True, mark_success=successes_delta > 0)
    if intervention_key:
        update_control_stat(user_id, "intervention", intervention_key, attempts_delta=attempts_delta, successes_delta=successes_delta, weighted_delta=weighted_delta, mark_used=True, mark_success=successes_delta > 0)
        parsed = parse_control_intervention_key(intervention_key)
        if parsed:
            update_control_stat(
                user_id,
                "intervention",
                f"{parsed['mode']}:{parsed['blocker']}:{parsed['pressure_level']}:{parsed['action_offer']}",
                attempts_delta=attempts_delta,
                successes_delta=successes_delta,
                weighted_delta=weighted_delta,
                mark_used=True,
                mark_success=successes_delta > 0,
            )
            update_control_stat(
                user_id,
                "intervention_parent",
                _parent_action_bucket(parsed["mode"], parsed["blocker"], parsed["action_offer"]),
                attempts_delta=attempts_delta,
                successes_delta=successes_delta,
                weighted_delta=weighted_delta,
                mark_used=True,
                mark_success=successes_delta > 0,
            )
            update_control_stat(
                user_id,
                "phrasing_style",
                f"{parsed['mode']}:{parsed['pressure_level']}:{parsed['phrasing_style']}",
                attempts_delta=attempts_delta,
                successes_delta=successes_delta,
                weighted_delta=weighted_delta,
                mark_used=True,
                mark_success=successes_delta > 0,
            )
    if pressure_level:
        update_control_stat(user_id, "pressure_level", pressure_level, attempts_delta=0 if outcome != "proactive_sent" else 1, successes_delta=successes_delta, weighted_delta=weighted_delta, mark_used=True, mark_success=successes_delta > 0)
    if pressure_level and event.get("message_type"):
        update_control_stat(user_id, "pressure_by_message", f"{event.get('message_type')}:{pressure_level}", attempts_delta=attempts_delta, successes_delta=successes_delta, weighted_delta=weighted_delta, mark_used=True, mark_success=successes_delta > 0)
    if pressure_level and phase:
        update_control_stat(user_id, "pressure_phase", f"{phase}:{pressure_level}", attempts_delta=attempts_delta, successes_delta=successes_delta, weighted_delta=weighted_delta, mark_used=True, mark_success=successes_delta > 0)
    if pressure_level and event.get("trigger"):
        update_control_stat(user_id, "pressure_trigger", f"{event.get('trigger')}:{pressure_level}", attempts_delta=attempts_delta, successes_delta=successes_delta, weighted_delta=weighted_delta, mark_used=True, mark_success=successes_delta > 0)
    if silence_reason:
        update_control_stat(user_id, "silence_reason", silence_reason, attempts_delta=1, successes_delta=0, weighted_delta=weighted_delta, mark_used=True)

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
    session_completed: bool = False,
    returned_same_day: bool = False,
    returned_next_day: bool = False,
    pressure_level: str | None = None,
    phrasing_style: str | None = None,
    action_offer: str | None = None,
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
        "session_completed": bool(session_completed),
        "returned_same_day": bool(returned_same_day),
        "returned_next_day": bool(returned_next_day),
        "pressure_level": pressure_level,
        "phrasing_style": phrasing_style,
        "action_offer": action_offer,
        "intervention_key": f"{trigger_type}:{mode}:{blocker or 'none'}:{pressure_level or 'medium'}:{action_offer or 'default'}:{phrasing_style or 'default'}",
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
    pending = get_pending_control(user_id) or {}
    pending_sent_at = ensure_aware(pending.get("sent_at"))
    if pending and pending_sent_at and ts >= pending_sent_at and not pending.get("resolved"):
        record_outcome(
            user_id,
            {
                "outcome_type": "user_response",
                "message_type": pending.get("message_type"),
                "trigger": pending.get("trigger"),
                "phase": pending.get("phase"),
                "intervention_key": pending.get("intervention_key"),
                "pressure_level": pending.get("pressure_level"),
                "responded": True,
            },
        )
        if pending.get("local_date") == local_now_for_user(user_id).date().isoformat():
            record_outcome(
                user_id,
                {
                    "outcome_type": "same_day_return",
                    "message_type": pending.get("message_type"),
                    "trigger": pending.get("trigger"),
                    "phase": pending.get("phase"),
                    "intervention_key": pending.get("intervention_key"),
                    "pressure_level": pending.get("pressure_level"),
                },
            )
        else:
            record_outcome(
                user_id,
                {
                    "outcome_type": "next_day_return",
                    "message_type": pending.get("message_type"),
                    "trigger": pending.get("trigger"),
                    "phase": pending.get("phase"),
                    "intervention_key": pending.get("intervention_key"),
                    "pressure_level": pending.get("pressure_level"),
                },
            )
        pending["resolved"] = True
        pending["responded_at"] = ts
        set_pending_control(user_id, pending)
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
    return current_utc_now()

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

TEST_USER_NAME_RE = re.compile(r"^test-\d+$")

def live_user_query() -> Dict[str, Any]:
    return {
        "is_test_user": {"$ne": True},
        "name": {"$not": TEST_USER_NAME_RE},
    }

def get_current_goal(user_id: int):
    return resolve_current_goal(user_id)

def effective_intention_goal(user_id: int) -> str | None:
    intention = get_today_intention(user_id) or {}
    selected_goal = intention.get("selected_goal")
    if selected_goal and is_goal_active(user_id, selected_goal):
        return selected_goal
    current = resolve_current_goal(user_id)
    return (current or {}).get("goal")

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
    record_outcome(
        user_id,
        {
            "outcome_type": "session_started",
            "message_type": "focus_session",
            "related_session_id": str(sid),
            "goal": g,
            "session_started": True,
            "progress_occurred": False,
        },
    )
    pending = get_pending_control(user_id) or {}
    if pending:
        pending["resolved"] = True
        pending["related_session_id"] = str(sid)
        set_pending_control(user_id, pending)
    return sid

def finish_latest_session(user_id: int, state: str = "DONE") -> bool:
    """Mark the most recent ACTIVE session as DONE/TIMEOUT/ABORTED."""
    s = sessions.find_one({"user_id": user_id, "state": "ACTIVE"}, sort=[("started_at", DESCENDING)])
    if not s:
        return False
    sessions.update_one({"_id": s["_id"]}, {"$set": {"state": state, "ends_at": now()}})
    log_event(user_id, "session_finish", {"sid": str(s["_id"]), "to": state})
    if state == "DONE":
        record_outcome(
            user_id,
            {
                "outcome_type": "session_completed",
                "message_type": "focus_session",
                "related_session_id": str(s["_id"]),
                "goal": s.get("goal"),
                "session_completed": True,
                "progress_occurred": True,
            },
        )
    pending = get_pending_control(user_id) or {}
    if pending:
        pending["resolved"] = True
        set_pending_control(user_id, pending)
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
    goal = intervention.get("goal") or effective_intention_goal(user_id) or "your target"
    tone_policy = intervention.get("tone_policy", "firm")
    phrasing_style = intervention.get("phrasing_style", "tactical")
    pressure_level = intervention.get("pressure_level", "medium")
    recent_phrases = ", ".join(recent_list_memory(user_id, "recent_phrase_signatures", limit=3)) or "none"
    prompt = (
        f"You are phrasing a deterministic Telegram accountability intervention.\n"
        f"Tone policy: {tone_policy}.\n"
        f"Phrasing style: {phrasing_style}.\n"
        f"Pressure level: {pressure_level}.\n"
        f"Goal: {goal}.\n"
        f"Trigger: {intervention.get('trigger')}.\n"
        f"Mode: {intervention.get('mode')}.\n"
        f"Blocker: {intervention.get('blocker') or 'none'}.\n"
        f"Action offer: {intervention.get('action_offer') or 'default'}.\n"
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
        {"$set": {"why": why, "status": "active", "completed_at": None, "updated_at": now()}},
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
    selected_goal = goal or effective_intention_goal(user_id)
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

def choose_tone_policy(user_id: int, trigger: str, *, blocker: str | None = None, pressure_level: str | None = None) -> str:
    recent_success = recent_success_count(user_id)
    avoidance = recent_avoidance_count(user_id)
    blocked = recent_blocked_sessions(user_id)
    severity = missed_day_severity(user_id)
    low_energy = top_bucket(get_memory(user_id, "time_of_day_slumps", {})) is not None and detect_blocker(user_id, blocker) == "tired"
    profile_style = ensure_profile(user_id).get("push_style", "firm")
    pressure = pressure_level or "medium"
    if trigger == "override":
        return "calm"
    if pressure == "low":
        return "calm"
    if pressure == "sharp":
        return "confrontational" if profile_style == "ruthless" else "blunt"
    if pressure == "high":
        return "blunt" if profile_style != "gentle" else "tactical"
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
    return choose_ranked_phrasing_style(user_id, mode, tone_policy, "medium")

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
    goal = effective_intention_goal(user_id) or "your goal"
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

    if trigger == "repeated_avoidance" and decay.get("action") in {"split", "replace"} and recent_avoidance_count(user_id) >= 2:
        mode = "clarity"

    precision_state = precision_reentry_state(user_id, {"trigger": trigger, "mode": mode, "blocker": blocker_name})
    pressure_level = choose_pressure_level(
        user_id,
        {
            "trigger": trigger,
            "blocker": blocker_name,
            "mode": mode,
            "phase": "intervention",
            "message_type": "intervention",
            "precision_reentry": precision_state.get("active"),
        },
    )
    candidates = candidate_intervention_actions(user_id, mode, trigger, blocker_name, goal, restart, decay)
    chosen = choose_ranked_candidate(user_id, mode, blocker_name, pressure_level, candidates)
    action = chosen["action"]
    action_offer = chosen["action_offer"]

    tone_policy = choose_tone_policy(user_id, trigger, blocker=blocker_name, pressure_level=pressure_level)
    if precision_state.get("active") and pressure_level != "low":
        tone_policy = "compressed"
    phrasing_style = choose_ranked_phrasing_style(user_id, mode, tone_policy, pressure_level, precision_reentry=precision_state.get("active", False))
    intervention_key = f"{mode}:{blocker_name}:{pressure_level}:{action_offer}:{phrasing_style}"

    result = {
        "trigger": trigger,
        "mode": mode,
        "blocker": blocker_name,
        "action": action,
        "action_offer": action_offer,
        "goal": goal,
        "restart_size_min": restart,
        "session_id": str(session_doc["_id"]) if session_doc else None,
        "tone_policy": tone_policy,
        "phrasing_style": phrasing_style,
        "pressure_level": pressure_level,
        "intervention_key": intervention_key,
        "goal_decay": decay,
        "precision_reentry": bool(precision_state.get("active")),
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

def daily_loop_hours_for_user(user_id: int) -> Dict[str, int]:
    profile = ensure_profile(user_id)
    user_doc = users.find_one({"user_id": user_id}) or {}
    precision = precision_reentry_state(user_id)
    anchor_hour = profile.get("loop_anchor_hour")
    if anchor_hour is None:
        anchor_hour = user_doc.get("checkin_hour")
    if anchor_hour is None:
        work_start = int(profile.get("work_start_hour", 9))
        anchor_hour = max(6, work_start - 1)
    anchor_hour = int(anchor_hour)
    morning = choose_best_time_window(user_id, {"phase": "morning", "default_hour": anchor_hour, "precision_reentry": precision.get("active")})
    midday = choose_best_time_window(user_id, {"phase": "midday", "default_hour": min(23, anchor_hour + 5), "precision_reentry": precision.get("active")})
    end_of_day = choose_best_time_window(user_id, {"phase": "eod", "default_hour": min(23, anchor_hour + 11), "precision_reentry": precision.get("active")})
    return {
        "morning": morning,
        "midday": midday,
        "eod": end_of_day,
    }

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
    loop_hours = daily_loop_hours_for_user(user_id)
    tz_name = get_user_timezone(user_id)
    return (
        f"Timezone: {tz_name}\n"
        f"Goals: {goals_list}\n"
        f"Push style: {profile.get('push_style', 'firm')}\n"
        f"Work start: {profile.get('work_start_hour', 9):02d}:00\n"
        f"Daily loop: {loop_hours['morning']:02d}:00 / {loop_hours['midday']:02d}:00 / {loop_hours['eod']:02d}:00 {tz_name}\n"
        f"Blockers: {blockers}\n"
        f"Restart size: {profile.get('restart_size_min', 10)} min"
    )

def intention_summary(user_id: int) -> str:
    intention = get_today_intention(user_id)
    if not intention:
        return "No daily intention yet."
    selected_goal = intention.get("selected_goal")
    if selected_goal and not is_goal_active(user_id, selected_goal):
        intention = upsert_today_intention(user_id, selected_goal=None)
        selected_goal = None
    return (
        f"Today's intention ({intention['date']})\n"
        f"Goal: {selected_goal or '—'}\n"
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
        goal = effective_intention_goal(user.id)
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
        return await update.message.reply_text("Set a goal first in /settings, then come back to focus.")

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
        return await update.message.reply_text("Goal setup now lives in /settings.")
    goal = context.args[0].lower()
    why = " ".join(context.args[1:])
    set_goal_why(user.id, goal, why)
    # set active if none exists
    u = users.find_one({"user_id": user.id}) or {}
    if not u.get("active_goal"):
        users.update_one({"user_id": user.id}, {"$set": {"active_goal": goal}}, upsert=True)
    log_event(user.id, "why", {"goal": goal})
    await update.message.reply_text(f"Saved: {goal} → “{why}”. Active goal: {goal}. Use /start or /settings to continue.")

async def cmd_setactive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user.id, user.full_name or "")
    touch_user(user.id, "command:setactive")
    if not context.args:
        return await update.message.reply_text("Use /goals to switch goals with buttons.")
    goal = context.args[0].lower()
    ok = set_active_goal(user.id, goal)
    if not ok:
        return await update.message.reply_text(f"No such active goal: {goal}. Use /goals to see your current list.")
    await update.message.reply_text(f"Active goal set to: {goal}")

async def cmd_goals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    touch_user(user.id, "command:goals")
    items = list_user_goals(user.id)
    if not items:
        completed_count = goals.count_documents({"user_id": user.id, "status": "done"})
        if completed_count:
            return await update.message.reply_text("No active goals right now. Finished goals are hidden from this list. Add a fresh one in /settings.")
        return await update.message.reply_text("No goals yet. Add your first one in /settings.")
    u = users.find_one({"user_id": user.id}) or {}
    active = u.get("active_goal")
    lst = "\n".join([f"• {g['goal']}" + ("  ← active" if g['goal']==active else "") for g in items])
    await update.message.reply_text(f"Your goals:\n{lst}", reply_markup=goals_list_buttons(user.id))

async def cmd_checkintime(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    touch_user(user.id, "command:checkintime")
    if not context.args:
        return await update.message.reply_text("Timing now lives in /settings.")
    try:
        hour = int(context.args[0])
        if not (0 <= hour <= 23):
            raise ValueError()
    except ValueError:
        return await update.message.reply_text("Enter an hour 0–23.")
    users.update_one({"user_id": user.id}, {"$set": {"checkin_hour": hour}}, upsert=True)
    set_profile_fields(user.id, loop_anchor_hour=hour)
    loop_hours = daily_loop_hours_for_user(user.id)
    tz_name = get_user_timezone(user.id)
    await update.message.reply_text(
        f"Daily loop anchor set to {hour:02d}:00 {tz_name}.\n"
        f"Morning: {loop_hours['morning']:02d}:00 | Midday: {loop_hours['midday']:02d}:00 | End-of-day: {loop_hours['eod']:02d}:00"
    )

async def cmd_checkin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user.id, user.full_name or "")
    touch_user(user.id, "command:checkin")
    g = resolve_current_goal(user.id)
    if not g:
        return await update.message.reply_text("Set a goal first in /settings.")
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
        return await update.message.reply_text("Set a goal first in /settings.")
    await run_override(user.id, g["goal"], context)

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    data = query.data
    ensure_user(user.id, user.full_name or user.username or "human")
    touch_user(user.id, f"callback:{data.split(':', 1)[0]}")
    record_outcome(user.id, {"outcome_type": "button_tap", "message_type": "callback", "trigger": data})

    if data == "noop":
        await query.answer()
        return

    if data == "menu:goals":
        items = list_user_goals(user.id)
        if not items:
            completed_count = goals.count_documents({"user_id": user.id, "status": "done"})
            if completed_count:
                await safe_edit_message_text(query, "No active goals right now. Finished goals are hidden here. Add a fresh one in /settings.")
            else:
                await safe_edit_message_text(query, "No goals yet. Add one in /settings.")
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
            await query.edit_message_text("Set at least one goal first in /settings.")
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
        selected_goal = intention.get("selected_goal")
        if status == "done" and selected_goal:
            mark_goal_status(user.id, selected_goal, "done")
            record_outcome(user.id, {"outcome_type": "progress_marked", "message_type": "daily_intention", "phase": "eod", "goal": selected_goal, "progress_occurred": True})
        elif status == "active" and selected_goal:
            mark_goal_status(user.id, selected_goal, "active")
        intention = upsert_today_intention(user.id, status=status)
        intention = get_today_intention(user.id) or intention
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
        summary = intention_summary(user.id)
        intention = get_today_intention(user.id) or intention
        await safe_edit_message_text(
            query,
            summary,
            reply_markup=intention_action_buttons(intention.get("status")),
        )
        return

    if data == "focus:begin":
        goal = effective_intention_goal(user.id)
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
        goal = effective_intention_goal(user.id)
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
        goal = effective_intention_goal(user.id) or "your target"
        await safe_edit_message_text(query, blocker_action(detect_blocker(user.id), 5, goal), reply_markup=focus_duration_buttons())
        return

    if data == "ux:start5":
        goal = effective_intention_goal(user.id)
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
        goal = effective_intention_goal(user.id)
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
            record_outcome(user.id, {"outcome_type": "progress_marked", "message_type": "focus_completion", "phase": "focus", "progress_occurred": True})
            await query.edit_message_text("Session logged as done. Keep the momentum.")
        elif outcome == "partial":
            upsert_today_intention(user.id, status="partial")
            log_event(user.id, "focus_completion", {"status": "partial"})
            record_intervention_outcome(user.id, trigger_type="focus_completion", mode="momentum", responded=True, session_started=True, progress_occurred=True, issue_repeated=False)
            record_outcome(user.id, {"outcome_type": "progress_marked", "message_type": "focus_completion", "phase": "focus", "progress_occurred": True})
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
            await query.edit_message_text("Set a goal first in /settings.")
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
        record_outcome(user.id, {"outcome_type": "repeated_avoidance", "message_type": "midday_prompt", "phase": "midday", "issue_repeated": True})
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
            record_outcome(user.id, {"outcome_type": "missed_day", "message_type": "eod_prompt", "phase": "eod", "issue_repeated": True})
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
        mark_goal_status(user.id, goal, "done")
        bump_streak(user.id, 1)
        udoc = users.find_one({"user_id": user.id}) or {}
        line = praise_line(udoc.get("streak", 0))
        log_event(user.id, "done", {"goal": goal})
        await query.edit_message_text(f"✅ Logged: {goal}. Goal marked done, so it will drop out of your active list. {line}")
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
    goal = effective_intention_goal(user.id) or (g["goal"] if g else "—")
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
    await deliver_message(
        context.bot,
        user_id,
        text=f"{step1}\n\n{step2}\n\n{step3}",
        message_type="override",
        phase="override",
        trigger="override",
    )

def loop_hours_for_user(user_id: int) -> Dict[str, int]:
    return daily_loop_hours_for_user(user_id)

async def send_proactive_message(
    app: Application,
    user_id: int,
    *,
    text: str,
    message_type: str,
    phase: str | None = None,
    trigger: str | None = None,
    reply_markup=None,
    parse_mode=None,
    intervention: Dict[str, Any] | None = None,
    related_session_id: str | None = None,
):
    decision = should_send_message(
        user_id,
        message_type,
        {
            "phase": phase,
            "trigger": trigger,
            "pressure_level": (intervention or {}).get("pressure_level"),
        },
    )
    if decision["decision"] != "send":
        record_outcome(
            user_id,
            {
                "outcome_type": f"message_{decision['decision']}",
                "message_type": message_type,
                "phase": phase,
                "trigger": trigger,
                "intervention_key": (intervention or {}).get("intervention_key"),
                "pressure_level": (intervention or {}).get("pressure_level"),
                "action_offer": (intervention or {}).get("action_offer"),
                "phrasing_style": (intervention or {}).get("phrasing_style"),
                "parent_action_key": _parent_action_bucket(
                    intervention.get("mode", "general"),
                    intervention.get("blocker", "none"),
                    intervention.get("action_offer", "default"),
                ) if intervention and intervention.get("action_offer") else None,
                "precision_reentry": bool((intervention or {}).get("precision_reentry")),
                "silence_reason": decision["reason"],
                "related_session_id": related_session_id,
            },
        )
        log_structured("message_suppressed", user_id=user_id, message_type=message_type, phase=phase, trigger=trigger, decision=decision["decision"], reason=decision["reason"])
        return False
    await deliver_message(
        app.bot,
        user_id,
        text=text,
        message_type=message_type,
        phase=phase,
        trigger=trigger,
        reply_markup=reply_markup,
        parse_mode=parse_mode,
        related_session_id=related_session_id,
    )
    local_date = local_now_for_user(user_id).date().isoformat()
    local_hour = local_now_for_user(user_id).hour
    pending = {
        "message_type": message_type,
        "phase": phase,
        "trigger": trigger,
        "intervention_key": (intervention or {}).get("intervention_key"),
        "pressure_level": (intervention or {}).get("pressure_level"),
        "action_offer": (intervention or {}).get("action_offer"),
        "phrasing_style": (intervention or {}).get("phrasing_style"),
        "parent_action_key": _parent_action_bucket(
            intervention.get("mode", "general"),
            intervention.get("blocker", "none"),
            intervention.get("action_offer", "default"),
        ) if intervention and intervention.get("action_offer") else None,
        "precision_reentry": bool((intervention or {}).get("precision_reentry")),
        "sent_at": now(),
        "local_date": local_date,
        "hour_bin": local_hour,
        "time_bucket": hour_time_bucket(local_hour),
        "resolved": False,
        "related_session_id": related_session_id,
    }
    set_pending_control(user_id, pending)
    if intervention and intervention.get("action_offer"):
        push_recent_memory(user_id, "recent_action_offers", str(intervention.get("action_offer")), limit=4, confidence=0.75)
    record_outcome(
        user_id,
        {
            "outcome_type": "proactive_sent",
            "message_type": message_type,
            "phase": phase,
            "trigger": trigger,
            "intervention_key": (intervention or {}).get("intervention_key"),
            "pressure_level": (intervention or {}).get("pressure_level"),
            "action_offer": (intervention or {}).get("action_offer"),
            "phrasing_style": (intervention or {}).get("phrasing_style"),
            "parent_action_key": _parent_action_bucket(
                intervention.get("mode", "general"),
                intervention.get("blocker", "none"),
                intervention.get("action_offer", "default"),
            ) if intervention and intervention.get("action_offer") else None,
            "precision_reentry": bool((intervention or {}).get("precision_reentry")),
            "related_session_id": related_session_id,
        },
    )
    return True

async def send_intervention_message(app: Application, user_id: int, trigger: str, *, blocker: str | None = None, session_doc: Dict[str, Any] | None = None, reply_markup=None):
    intervention = choose_intervention(user_id, trigger, blocker=blocker, session_doc=session_doc)
    text = phrase_intervention(user_id, intervention)
    sent = await send_proactive_message(
        app,
        user_id,
        text=text,
        message_type="intervention",
        phase="intervention",
        trigger=trigger,
        reply_markup=reply_markup or premium_action_buttons(user_id, intervention),
        intervention=intervention,
        related_session_id=str(session_doc["_id"]) if session_doc else None,
    )
    if not sent:
        return False
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
        pressure_level=intervention.get("pressure_level"),
        phrasing_style=intervention.get("phrasing_style"),
        action_offer=intervention.get("action_offer"),
    )
    return True

async def run_daily_loop_for_user(app: Application, uid: int):
    ensure_profile(uid, (users.find_one({"user_id": uid}) or {}).get("name", "human"))
    local_now = current_utc_now().astimezone(ZoneInfo(get_user_timezone(uid)))
    hour = local_now.hour
    hours = loop_hours_for_user(uid)
    intention = get_today_intention(uid) or {}
    yesterday = get_intention_for_date(uid, date_key_for_user(uid, -1)) or {}
    state_doc = get_state(uid)
    current_goal = resolve_current_goal(uid)
    last_touch = ensure_aware(state_doc.get("last_user_touch_at"))

    if yesterday.get("status") == "missed" and hour >= hours["morning"] and not intention.get("missed_day_recovery_sent_at"):
        sent = await send_intervention_message(app, uid, "missed_day", reply_markup=intervention_reply_markup(uid, "missed_day"))
        if sent:
            upsert_today_intention(uid, missed_day_recovery_sent_at=now(), morning_prompt_sent_at=intention.get("morning_prompt_sent_at") or now())
        return

    if hour >= hours["morning"] and not intention.get("morning_prompt_sent_at"):
        sent = await send_proactive_message(
            app,
            uid,
            text=morning_summary_text(uid),
            message_type="morning_prompt",
            phase="morning",
            reply_markup=morning_anchor_buttons(),
        )
        if sent:
            upsert_today_intention(uid, morning_prompt_sent_at=now(), status=intention.get("status") or "planned")
            log_structured("morning_prompt_sent", user_id=uid, hour=hour, date=today_key_for_user(uid))
            log_event(uid, "daily_loop", {"phase": "morning_anchor"})
        return

    morning_sent_at = ensure_aware(intention.get("morning_prompt_sent_at"))
    if morning_sent_at and not intention.get("morning_response_at") and now() >= morning_sent_at + timedelta(hours=2):
        if not last_touch or last_touch <= morning_sent_at:
            if not intention.get("morning_followup_sent_at"):
                record_outcome(uid, {"outcome_type": "no_response", "message_type": "morning_prompt", "phase": "morning"})
                sent = await send_intervention_message(app, uid, "no_response_after_morning_prompt", reply_markup=morning_anchor_buttons())
                if sent:
                    upsert_today_intention(uid, morning_followup_sent_at=now())
                return

    if hour >= hours["midday"] and intention.get("target") and not intention.get("midday_prompt_sent_at"):
        sent = await send_proactive_message(
            app,
            uid,
            text="Midday check. Where are you at?",
            message_type="midday_prompt",
            phase="midday",
            reply_markup=midday_check_buttons(),
        )
        if sent:
            upsert_today_intention(uid, midday_prompt_sent_at=now())
            log_structured("midday_prompt_sent", user_id=uid, hour=hour, goal=intention.get("selected_goal"))
            log_event(uid, "daily_loop", {"phase": "midday"})
        return

    if recent_avoidance_count(uid) >= 2 and not intention.get("avoidance_recovery_sent_at"):
        record_outcome(uid, {"outcome_type": "repeated_avoidance", "message_type": "intervention", "phase": "intervention", "issue_repeated": True})
        sent = await send_intervention_message(app, uid, "repeated_avoidance", reply_markup=intervention_reply_markup(uid, "repeated_avoidance"))
        if sent:
            upsert_today_intention(uid, avoidance_recovery_sent_at=now())
        return

    target_updated_at = ensure_aware(intention.get("updated_at"))
    if intention.get("target") and intention.get("status") in {"planned", "active", "partial", "blocked"} and not get_active_session(uid):
        if target_updated_at and now() >= target_updated_at + timedelta(minutes=90):
            if not intention.get("target_inactivity_sent_at") and (not last_touch or last_touch <= target_updated_at):
                record_outcome(uid, {"outcome_type": "no_response", "message_type": "midday_prompt", "phase": "midday"})
                sent = await send_intervention_message(app, uid, "inactivity_after_target", reply_markup=focus_duration_buttons())
                if sent:
                    upsert_today_intention(uid, target_inactivity_sent_at=now())
                return

    if hour >= hours["eod"] and intention.get("target") and not intention.get("eod_prompt_sent_at"):
        sent = await send_proactive_message(
            app,
            uid,
            text="End of day check. How did it go?",
            message_type="eod_prompt",
            phase="eod",
            reply_markup=end_of_day_buttons(),
        )
        if sent:
            upsert_today_intention(uid, eod_prompt_sent_at=now())
            log_structured("eod_prompt_sent", user_id=uid, hour=hour, goal=intention.get("selected_goal"))
            log_event(uid, "daily_loop", {"phase": "eod"})
        return

    goal_updated_at = ensure_aware((current_goal or {}).get("updated_at"))
    if current_goal and goal_updated_at and now() >= goal_updated_at + timedelta(days=7):
        if not intention and not state_doc.get("stale_goal_sent_at"):
            maybe_log_goal_decay(uid, current_goal.get("goal"))
            trigger = "goal_decay" if detect_goal_decay(uid, current_goal.get("goal")).get("decayed") else "stale_goal"
            sent = await send_intervention_message(
                app,
                uid,
                trigger,
                reply_markup=intervention_reply_markup(uid, trigger),
            )
            if sent:
                state.update_one({"user_id": uid}, {"$set": {"stale_goal_sent_at": now()}}, upsert=True)

async def run_daily_loop_service(app: Application):
    for u in users.find(live_user_query()):
        uid = u["user_id"]
        try:
            await run_daily_loop_for_user(app, uid)
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
    for u in users.find(live_user_query()):
        uid = u["user_id"]
        try:
            facts = weekly_summary_facts(uid)
            msg = phrase_weekly_summary(uid, facts)
            await deliver_message(
                app.bot,
                uid,
                text=msg,
                message_type="weekly_summary",
                phase="weekly",
                trigger="weekly_summary",
            )
            log_structured("weekly_summary_sent", user_id=uid, days_active=facts.get("days_active"), main_blocker=facts.get("main_blocker_pattern"), what_worked=facts.get("what_worked"))
            log_event(uid, "insight", facts)
            set_memory(uid, "last_weekly_summary", facts, 0.85)
        except Exception:
            logger.exception("Weekly summary failed for user_id=%s", uid)
    log_structured("cron_weekly_finish")

def test_clock_payload() -> Dict[str, Any]:
    fake = system_state.find_one({"_id": "clock"}) or {}
    fake_value = fake.get("fake_utc_now")
    if isinstance(fake_value, str):
        fake_utc = parse_iso_dt(fake_value).astimezone(dt.timezone.utc)
    else:
        fake_utc = ensure_aware(fake_value)
    return {
        "fake_utc_now": fake_utc.isoformat() if fake_utc else None,
        "effective_now_default_tz": now().isoformat(),
        "updated_at": (ensure_aware(fake.get("updated_at")) or now()).isoformat() if fake else None,
    }

def get_test_mode() -> Dict[str, Any]:
    return system_state.find_one({"_id": "test_mode"}) or {}

def set_test_mode(*, suppress_telegram: bool = False, scenario: str | None = None, user_id: int | None = None):
    system_state.update_one(
        {"_id": "test_mode"},
        {"$set": {
            "suppress_telegram": bool(suppress_telegram),
            "scenario": scenario,
            "user_id": user_id,
            "updated_at": now(),
        }},
        upsert=True,
    )
    return get_test_mode()

def clear_test_mode():
    system_state.delete_one({"_id": "test_mode"})

def clear_test_outbox(user_id: int):
    test_outbox.delete_many({"user_id": user_id})

def get_test_outbox(user_id: int, limit: int = 20) -> list[Dict[str, Any]]:
    return list(test_outbox.find({"user_id": user_id}).sort("ts", DESCENDING).limit(limit))

async def deliver_message(
    bot,
    user_id: int,
    *,
    text: str,
    message_type: str,
    phase: str | None = None,
    trigger: str | None = None,
    reply_markup=None,
    parse_mode=None,
    related_session_id: str | None = None,
):
    test_mode = get_test_mode()
    if test_mode.get("scenario"):
        test_outbox.insert_one({
            "user_id": user_id,
            "ts": now(),
            "updated_at": now(),
            "text": text,
            "message_type": message_type,
            "phase": phase,
            "trigger": trigger,
            "related_session_id": related_session_id,
            "parse_mode": parse_mode,
            "scenario": test_mode.get("scenario"),
            "delivered_to_telegram": not bool(test_mode.get("suppress_telegram")),
        })
        log_structured("test_outbox_capture", user_id=user_id, message_type=message_type, phase=phase, trigger=trigger)
    if test_mode.get("suppress_telegram"):
        return {"captured": True}
    return await bot.send_message(chat_id=user_id, text=text, reply_markup=reply_markup, parse_mode=parse_mode)

def mongo_safe(value: Any):
    return jsonable_encoder(
        value,
        custom_encoder={
            ObjectId: str,
            dt.datetime: lambda v: ensure_aware(v).isoformat() if ensure_aware(v) else None,
        },
    )

def set_test_clock(iso_value: str | None):
    if not iso_value:
        system_state.delete_one({"_id": "clock"})
        return test_clock_payload()
    fake_utc = parse_iso_dt(iso_value).astimezone(dt.timezone.utc)
    system_state.update_one(
        {"_id": "clock"},
        {"$set": {"fake_utc_now": fake_utc.isoformat(), "updated_at": now()}},
        upsert=True,
    )
    return test_clock_payload()

def reset_user_test_data(user_id: int):
    goals.delete_many({"user_id": user_id})
    logs.delete_many({"user_id": user_id})
    state.delete_many({"user_id": user_id})
    sessions.delete_many({"user_id": user_id})
    events.delete_many({"user_id": user_id})
    daily_intentions.delete_many({"user_id": user_id})
    memory.delete_many({"user_id": user_id})
    intervention_outcomes.delete_many({"user_id": user_id})
    control_stats.delete_many({"user_id": user_id})
    control_events.delete_many({"user_id": user_id})
    profiles.delete_many({"user_id": user_id})
    users.delete_many({"user_id": user_id})

def seed_test_user(user_id: int, *, timezone: str = "America/Toronto"):
    ensure_user(user_id, f"test-{user_id}")
    set_profile_fields(
        user_id,
        timezone=timezone,
        push_style="firm",
        work_start_hour=9,
        loop_anchor_hour=8,
        blockers=["distracted", "overwhelmed"],
        restart_size_min=10,
        onboarding_complete=True,
        conversation=None,
    )
    users.update_one({"user_id": user_id}, {"$set": {"tz": timezone, "checkin_hour": 8, "is_test_user": True}}, upsert=True)
    set_goal_why(user_id, "optimization-of-brobot", "ship a cleaner bot")
    set_goal_why(user_id, "health", "protect energy")
    users.update_one({"user_id": user_id}, {"$set": {"active_goal": "optimization-of-brobot"}}, upsert=True)

DAILY_LOOP_SCENARIOS = {
    "fresh_morning",
    "midday_active",
    "missed_day_recovery",
    "repeated_avoidance",
    "stale_goal",
    "morning_followup_tired",
    "anxious_restart",
    "active_session_shield",
    "low_yield_burst",
    "adaptive_morning_shift",
    "goal_decay_replace",
    "west_coast_morning",
    "evening_wrapup",
    "ruthless_avoidance",
}

SESSION_TICK_SCENARIOS = {
    "blocked_focus",
    "focus_nudge_start",
    "focus_nudge_mid_session",
}

PASSIVE_SCENARIOS = {
    "onboarding_dropoff",
    "onboarding_manual_timezone",
}

def seed_scenario(user_id: int, scenario: str, *, reset: bool = True) -> Dict[str, Any]:
    if reset:
        reset_user_test_data(user_id)
    seed_test_user(user_id)
    today_key = today_key_for_user(user_id)
    yesterday_key = date_key_for_user(user_id, -1)
    scenario = str(scenario or "").strip().lower()

    if scenario == "fresh_morning":
        pass
    elif scenario == "midday_active":
        upsert_today_intention(
            user_id,
            selected_goal="optimization-of-brobot",
            target="finish phase 1 cleanup",
            fallback="do one visible cleanup move",
            status="active",
            morning_prompt_sent_at=now() - timedelta(hours=3),
            morning_response_at=now() - timedelta(hours=2, minutes=50),
        )
    elif scenario == "missed_day_recovery":
        daily_intentions.update_one(
            {"user_id": user_id, "date": yesterday_key},
            {"$set": {
                "user_id": user_id,
                "date": yesterday_key,
                "timezone": get_user_timezone(user_id),
                "selected_goal": "optimization-of-brobot",
                "target": "finish phase 1 cleanup",
                "fallback": "smallest cleanup move",
                "status": "missed",
                "created_at": now() - timedelta(days=1),
                "updated_at": now() - timedelta(days=1),
            }},
            upsert=True,
        )
    elif scenario == "repeated_avoidance":
        upsert_today_intention(
            user_id,
            selected_goal="optimization-of-brobot",
            target="finish phase 2 integration",
            fallback="start 5 minutes",
            status="active",
            morning_prompt_sent_at=now() - timedelta(hours=4),
            morning_response_at=now() - timedelta(hours=3, minutes=40),
            midday_prompt_sent_at=now() - timedelta(minutes=70),
            target_inactivity_sent_at=now() - timedelta(minutes=60),
        )
        for idx in range(3):
            logs.insert_one({
                "user_id": user_id,
                "ts": now() - timedelta(hours=idx + 1),
                "kind": "loop_status",
                "data": {"phase": "midday", "status": "avoiding"},
            })
    elif scenario == "stale_goal":
        goals.update_one(
            {"user_id": user_id, "goal": "optimization-of-brobot"},
            {"$set": {"updated_at": now() - timedelta(days=8), "status": "active"}},
        )
    elif scenario == "morning_followup_tired":
        set_profile_fields(user_id, blockers=["tired"], push_style="gentle")
        state.update_one({"user_id": user_id}, {"$set": {"mood": "tired"}}, upsert=True)
        upsert_today_intention(
            user_id,
            selected_goal="optimization-of-brobot",
            target="finish phase 1 cleanup",
            fallback="do one smallest useful move",
            status="planned",
            morning_prompt_sent_at=now() - timedelta(hours=3),
        )
    elif scenario == "anxious_restart":
        set_profile_fields(user_id, blockers=["anxious"], push_style="gentle")
        state.update_one({"user_id": user_id}, {"$set": {"mood": "anxious"}}, upsert=True)
        daily_intentions.update_one(
            {"user_id": user_id, "date": yesterday_key},
            {"$set": {
                "user_id": user_id,
                "date": yesterday_key,
                "timezone": get_user_timezone(user_id),
                "selected_goal": "optimization-of-brobot",
                "target": "ship validation polish",
                "fallback": "one ugly first step",
                "status": "missed",
                "created_at": now() - timedelta(days=1),
                "updated_at": now() - timedelta(days=1),
            }},
            upsert=True,
        )
    elif scenario == "active_session_shield":
        upsert_today_intention(
            user_id,
            selected_goal="optimization-of-brobot",
            target="finish implementation pass",
            fallback="reduce to one method",
            status="active",
            morning_prompt_sent_at=now() - timedelta(hours=4),
            morning_response_at=now() - timedelta(hours=3, minutes=45),
        )
        sid = start_session(user_id, 25, "optimization-of-brobot", nudges_enabled=True, source="test_seed")
        sessions.update_one(
            {"_id": sid},
            {"$set": {
                "started_confirmed": True,
                "ends_at": now() + timedelta(minutes=20),
                "next_check_at": now() + timedelta(minutes=10),
            }},
        )
    elif scenario == "low_yield_burst":
        upsert_today_intention(
            user_id,
            selected_goal="optimization-of-brobot",
            target="finish risky refactor",
            fallback="touch one safe call site",
            status="active",
            morning_prompt_sent_at=now() - timedelta(hours=4),
            morning_response_at=now() - timedelta(hours=3, minutes=40),
        )
        for idx in range(3):
            record_outcome(
                user_id,
                {
                    "outcome_type": "proactive_sent",
                    "message_type": "intervention",
                    "phase": "intervention",
                    "ts": now() - timedelta(minutes=90 - (idx * 15)),
                },
            )
        clear_pending_control(user_id)
    elif scenario == "adaptive_morning_shift":
        set_profile_fields(user_id, loop_anchor_hour=8)
        set_memory(user_id, "time_of_day_activity", {"9": 5, "8": 1}, 0.8)
        set_memory(user_id, "time_of_day_slumps", {"8": 4}, 0.8)
        update_control_stat(user_id, "timing_hour", "morning:9", attempts_delta=4, successes_delta=4, weighted_delta=2.4, mark_used=True, mark_success=True)
        update_control_stat(user_id, "timing_hour", "morning:8", attempts_delta=4, successes_delta=0, weighted_delta=-1.2, mark_used=True, mark_success=False)
    elif scenario == "goal_decay_replace":
        goals.update_one(
            {"user_id": user_id, "goal": "optimization-of-brobot"},
            {"$set": {"updated_at": now() - timedelta(days=8), "status": "active"}},
        )
        for days_ago, status in enumerate(["blocked", "missed", "partial", "blocked"]):
            date_key = date_key_for_user(user_id, -days_ago)
            daily_intentions.update_one(
                {"user_id": user_id, "date": date_key},
                {"$set": {
                    "user_id": user_id,
                    "date": date_key,
                    "timezone": get_user_timezone(user_id),
                    "selected_goal": "optimization-of-brobot",
                    "target": f"dragging-target-{days_ago}",
                    "fallback": "reduce to one visible move",
                    "status": status,
                    "created_at": now() - timedelta(days=days_ago),
                    "updated_at": now() - timedelta(days=days_ago),
                }},
                upsert=True,
            )
        for idx in range(3):
            logs.insert_one({
                "user_id": user_id,
                "ts": now() - timedelta(hours=idx + 1),
                "kind": "loop_status",
                "data": {"phase": "midday", "status": "avoiding"},
            })
    elif scenario == "west_coast_morning":
        set_profile_fields(user_id, timezone="America/Los_Angeles")
        users.update_one({"user_id": user_id}, {"$set": {"tz": "America/Los_Angeles", "checkin_hour": 8}}, upsert=True)
    elif scenario == "evening_wrapup":
        upsert_today_intention(
            user_id,
            selected_goal="optimization-of-brobot",
            target="ship evening cleanup",
            fallback="one visible cleanup move",
            status="active",
            morning_prompt_sent_at=now() - timedelta(hours=10),
            morning_response_at=now() - timedelta(hours=9, minutes=45),
            midday_prompt_sent_at=now() - timedelta(hours=5),
        )
    elif scenario == "ruthless_avoidance":
        set_profile_fields(user_id, push_style="ruthless", blockers=["perfectionist"])
        upsert_today_intention(
            user_id,
            selected_goal="optimization-of-brobot",
            target="ship phase 4 rewrite",
            fallback="push a rough draft",
            status="active",
            morning_prompt_sent_at=now() - timedelta(hours=5),
            morning_response_at=now() - timedelta(hours=4, minutes=45),
            midday_prompt_sent_at=now() - timedelta(minutes=80),
            target_inactivity_sent_at=now() - timedelta(minutes=70),
        )
        for idx in range(4):
            logs.insert_one({
                "user_id": user_id,
                "ts": now() - timedelta(minutes=(idx + 1) * 30),
                "kind": "loop_status",
                "data": {"phase": "midday", "status": "avoiding"},
            })
    elif scenario == "focus_nudge_start":
        upsert_today_intention(
            user_id,
            selected_goal="optimization-of-brobot",
            target="start focused validation",
            fallback="open the failing test",
            status="active",
        )
        sid = start_session(user_id, 25, "optimization-of-brobot", nudges_enabled=True, source="test_seed")
        sessions.update_one(
            {"_id": sid},
            {"$set": {
                "started_confirmed": False,
                "ends_at": now() + timedelta(minutes=20),
                "next_check_at": now() - timedelta(minutes=1),
                "nudges_sent": 0,
                "asked_completion": False,
            }},
        )
    elif scenario == "focus_nudge_mid_session":
        upsert_today_intention(
            user_id,
            selected_goal="optimization-of-brobot",
            target="stay in the pocket",
            fallback="resume one file",
            status="active",
        )
        sid = start_session(user_id, 25, "optimization-of-brobot", nudges_enabled=True, source="test_seed")
        sessions.update_one(
            {"_id": sid},
            {"$set": {
                "started_confirmed": True,
                "ends_at": now() + timedelta(minutes=20),
                "next_check_at": now() - timedelta(minutes=1),
                "nudges_sent": 1,
                "asked_completion": False,
            }},
        )
    elif scenario == "blocked_focus":
        upsert_today_intention(
            user_id,
            selected_goal="optimization-of-brobot",
            target="finish phase 3 validation",
            fallback="smallest validation move",
            status="active",
        )
        sid = start_session(user_id, 25, "optimization-of-brobot", nudges_enabled=True, source="test_seed")
        sessions.update_one(
            {"_id": sid},
            {"$set": {
                "started_confirmed": True,
                "ends_at": now() - timedelta(minutes=1),
                "next_check_at": now() - timedelta(minutes=1),
                "nudges_sent": 1,
                "asked_completion": False,
            }},
        )
    elif scenario == "weekly_summary":
        for days_ago, status in enumerate(["done", "partial", "missed", "done", "active", "missed", "done"]):
            date_key = date_key_for_user(user_id, -days_ago)
            daily_intentions.update_one(
                {"user_id": user_id, "date": date_key},
                {"$set": {
                    "user_id": user_id,
                    "date": date_key,
                    "timezone": get_user_timezone(user_id),
                    "selected_goal": "optimization-of-brobot",
                    "target": f"target-{days_ago}",
                    "fallback": "fallback",
                    "status": status,
                    "created_at": now() - timedelta(days=days_ago),
                    "updated_at": now() - timedelta(days=days_ago),
                }},
                upsert=True,
            )
        for idx in range(4):
            record_intervention_outcome(
                user_id,
                trigger_type="test_seed",
                mode="focus" if idx % 2 == 0 else "recovery",
                blocker="distracted" if idx % 2 == 0 else "overwhelmed",
                responded=True,
                session_started=idx % 2 == 0,
                progress_occurred=idx % 2 == 0,
                issue_repeated=idx % 2 == 1,
            )
    elif scenario == "onboarding_dropoff":
        reset_user_test_data(user_id)
        ensure_user(user_id, f"test-{user_id}")
        stale_created = now() - timedelta(hours=25)
        set_profile_fields(user_id, timezone="America/Toronto", onboarding_complete=False, conversation={"kind": "onboarding", "step": "goal_name", "data": {}}, created_at=stale_created)
        users.update_one({"user_id": user_id}, {"$set": {"created_at": stale_created}}, upsert=True)
        profiles.update_one({"user_id": user_id}, {"$set": {"created_at": stale_created}}, upsert=True)
    elif scenario == "onboarding_manual_timezone":
        reset_user_test_data(user_id)
        ensure_user(user_id, f"test-{user_id}")
        stale_created = now() - timedelta(hours=2)
        set_profile_fields(user_id, timezone="America/Toronto", onboarding_complete=False, conversation={"kind": "onboarding", "step": "timezone_text", "data": {"goal_count": 0}}, created_at=stale_created)
        users.update_one({"user_id": user_id}, {"$set": {"created_at": stale_created}}, upsert=True)
        profiles.update_one({"user_id": user_id}, {"$set": {"created_at": stale_created}}, upsert=True)
    else:
        raise HTTPException(status_code=400, detail=f"Unknown scenario: {scenario}")

    return {
        "user_id": user_id,
        "scenario": scenario,
        "today": today_key,
        "timezone": get_user_timezone(user_id),
        "current_goal": (resolve_current_goal(user_id) or {}).get("goal"),
        "intention": get_today_intention(user_id),
    }

def ops_summary_payload(hours: int = 24, user_id: int | None = None) -> Dict[str, Any]:
    cutoff = recent_cutoff(hours)
    base_query: Dict[str, Any] = {"ts": {"$gte": cutoff}}
    if user_id is not None:
        base_query["user_id"] = user_id
    daily_loop_logs = list(logs.find({"kind": "daily_loop", **base_query}))
    intervention_logs = list(logs.find({"kind": "intervention", **base_query}))
    loop_status_logs = list(logs.find({"kind": "loop_status", **base_query}))
    focus_logs = list(logs.find({"kind": "focus_completion", **base_query}))
    focus_prompt_logs = list(logs.find({"kind": "focus_completion_prompt", **base_query}))
    session_finishes = list(logs.find({"kind": "session_finish", **base_query}))
    outcomes = list(intervention_outcomes.find(base_query))
    control = list(control_events.find(base_query))

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

    control_counts: Dict[str, int] = {}
    for item in control:
        outcome_type = item.get("outcome_type")
        if outcome_type:
            control_counts[outcome_type] = control_counts.get(outcome_type, 0) + 1

    onboarding_dropoff_query: Dict[str, Any] = {
        "onboarding_complete": False,
        "created_at": {"$lte": now() - timedelta(hours=24)},
    }
    if user_id is not None:
        onboarding_dropoff_query["user_id"] = user_id
    onboarding_dropoff = count_docs(profiles, onboarding_dropoff_query)

    return {
        "window_hours": hours,
        "timezone_default": TZ,
        "prompt_delivery": {
            "daily_loop": daily_loop_phase_counts,
            "intervention_sends": len(intervention_logs),
            "focus_completion_prompts": len(focus_prompt_logs),
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
        "control": {
            "events": control_counts,
            "stats": count_docs(control_stats, {"user_id": user_id} if user_id is not None else {}),
        },
        "onboarding": {
            "incomplete_profiles": count_docs(profiles, {"onboarding_complete": False, **({"user_id": user_id} if user_id is not None else {})}),
            "dropoff_24h": onboarding_dropoff,
        },
        "sessions": {
            "active": count_docs(sessions, {"state": "ACTIVE", **({"user_id": user_id} if user_id is not None else {})}),
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
tg_app.add_handler(CommandHandler("override", cmd_override))
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
        last_error_date = getattr(info, "last_error_date", None)
        if isinstance(last_error_date, dt.datetime):
            last_error_date = (ensure_aware(last_error_date) or last_error_date).isoformat()
        webhook_info = {
            "url": getattr(info, "url", ""),
            "pending_update_count": getattr(info, "pending_update_count", None),
            "last_error_date": last_error_date,
            "last_error_message": getattr(info, "last_error_message", None),
        }
    except Exception:
        logger.exception("Webhook verification failed")
    return JSONResponse(mongo_safe({
        "mongo_ok": mongo_ok,
        "expected_webhook_url": f"{expected_base}/webhook" if expected_base else "",
        "webhook": webhook_info,
        "cron_secret_configured": bool(CRON_SECRET),
        "timezone_default": TZ,
        "ops_summary_24h": ops_summary_payload(24),
    }))

@app.get("/dev/clock")
async def dev_clock_get(request: Request):
    _check_cron_auth(request)
    return JSONResponse(test_clock_payload())

@app.post("/dev/clock")
async def dev_clock_set(request: Request):
    _require_api_secret(request)
    data = await request.json()
    iso_value = data.get("iso")
    if data.get("reset"):
        iso_value = None
    return JSONResponse(set_test_clock(iso_value))

@app.post("/dev/scenarios/seed")
async def dev_seed_scenario(request: Request):
    _require_api_secret(request)
    data = await request.json()
    user_id = int(data.get("user_id"))
    scenario = str(data.get("scenario") or "").strip()
    reset = bool(data.get("reset", True))
    result = seed_scenario(user_id, scenario, reset=reset)
    return mongo_safe(result)

@app.post("/dev/scenarios/run")
async def dev_run_scenario(request: Request):
    _require_api_secret(request)
    data = await request.json()
    user_id = int(data.get("user_id"))
    scenario = str(data.get("scenario") or "").strip()
    reset = bool(data.get("reset", True))
    suppress_telegram = bool(data.get("suppress_telegram", True))
    clear_test_outbox(user_id)
    set_test_mode(suppress_telegram=suppress_telegram, scenario=scenario, user_id=user_id)
    result = seed_scenario(user_id, scenario, reset=reset)
    try:
        if scenario in DAILY_LOOP_SCENARIOS:
            await run_daily_loop_for_user(tg_app, user_id)
        elif scenario in SESSION_TICK_SCENARIOS:
            for s in list(sessions.find({"user_id": user_id, "state": "ACTIVE"})):
                await run_session_tick_for_doc(tg_app, s)
        elif scenario == "weekly_summary":
            facts = weekly_summary_facts(user_id)
            msg = phrase_weekly_summary(user_id, facts)
            await deliver_message(
                tg_app.bot,
                user_id,
                text=msg,
                message_type="weekly_summary",
                phase="weekly",
                trigger="weekly_summary",
            )
            log_structured("weekly_summary_sent", user_id=user_id, days_active=facts.get("days_active"), main_blocker=facts.get("main_blocker_pattern"), what_worked=facts.get("what_worked"))
            log_event(user_id, "insight", facts)
            set_memory(user_id, "last_weekly_summary", facts, confidence=0.8)
        return mongo_safe({
            "seed": result,
            "clock": test_clock_payload(),
            "ops_summary_24h": ops_summary_payload(24, user_id=user_id),
            "test_outbox": get_test_outbox(user_id),
        })
    finally:
        clear_test_mode()

@app.post("/dev/outcomes/record")
async def dev_record_outcome(request: Request):
    _require_api_secret(request)
    data = await request.json()
    user_id = int(data.get("user_id"))
    event = {k: v for k, v in data.items() if k != "user_id"}
    return mongo_safe(record_outcome(user_id, event))

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

async def run_session_tick_for_doc(app: Application, s: Dict[str, Any]):
    now_utc = now()
    uid = s["user_id"]
    ends_at = ensure_aware(s.get("ends_at")) or now_utc
    if now_utc >= ends_at and not s.get("asked_completion", False):
        try:
            sent = await send_proactive_message(
                app,
                uid,
                text=f"Time's up for {_session_msg_goal_line(s)}. How did it go?",
                message_type="session_completion",
                phase="focus",
                reply_markup=focus_completion_buttons(),
                parse_mode="Markdown",
                related_session_id=str(s["_id"]),
            )
            if sent:
                log_structured("session_completion_prompt_sent", user_id=uid, session_id=str(s["_id"]), goal=s.get("goal"))
                log_event(uid, "focus_completion_prompt", {"status": "asked", "sid": str(s["_id"])})
                sessions.update_one({"_id": s["_id"]}, {"$set": {"asked_completion": True}})
        except Exception:
            logger.exception("Session completion prompt failed for user_id=%s", uid)
        return

    nca = ensure_aware(s.get("next_check_at"))
    if not s.get("nudges_enabled", True) or not nca or now_utc < nca:
        return

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
        return

    try:
        sent = await send_proactive_message(
            app,
            uid,
            text=txt,
            message_type="session_nudge",
            phase="focus",
            reply_markup=kb,
            parse_mode="Markdown",
            related_session_id=str(s["_id"]),
        )
        if sent:
            log_structured("session_nudge_sent", user_id=uid, session_id=str(s["_id"]), started=started, nudges_sent=nudges + 1)
            sessions.update_one({"_id": s["_id"]}, {"$set": {"next_check_at": next_dt}, "$inc": {"nudges_sent": 1}})
        else:
            sessions.update_one({"_id": s["_id"]}, {"$set": {"next_check_at": next_dt}})
    except Exception:
        logger.exception("Session tick failed for user_id=%s", uid)

async def cron_sessions_tick(app: Application):
    log_structured("cron_sessions_tick_start")
    active = list(sessions.find({"state": "ACTIVE"}))
    for s in active:
        await run_session_tick_for_doc(app, s)
    log_structured("cron_sessions_tick_finish", active_sessions=len(active))

# Endpoint to trigger it (like your other cron endpoints)
@app.get("/cron/sessions-tick")
async def cron_sessions_tick_endpoint(request: Request):
    _check_cron_auth(request)
    await cron_sessions_tick(tg_app)
    return PlainTextResponse("sessions-tick-ok")
