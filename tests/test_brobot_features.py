import importlib
import os
import unittest
from pathlib import Path


def _load_dotenv(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


REPO_ROOT = Path(__file__).resolve().parents[1]
_load_dotenv(REPO_ROOT / ".env")

for env_name in ("BOT_TOKEN", "MONGO_URI", "COHERE_API_KEY"):
    if not os.environ.get(env_name):
        raise RuntimeError(f"Missing required environment variable for tests: {env_name}")

bot = importlib.import_module("Telegram_Bot")
dev_scenarios = importlib.import_module("dev_scenarios")
_ORIGINAL_COHERE_CHAT = getattr(bot.co, "chat", None)


class _FakeCohereResponse:
    def __init__(self, text: str):
        self.text = text


def setUpModule():
    if _ORIGINAL_COHERE_CHAT is not None:
        bot.co.chat = lambda *args, **kwargs: _FakeCohereResponse("Deterministic test phrasing.")


def tearDownModule():
    if _ORIGINAL_COHERE_CHAT is not None:
        bot.co.chat = _ORIGINAL_COHERE_CHAT
    try:
        bot.mongo.close()
    except Exception:
        pass


def _run_weekly_summary(user_id: int) -> None:
    facts = bot.weekly_summary_facts(user_id)
    message = f"Weekly summary\nDays active: {facts.get('days_active', 0)}"
    bot.test_outbox.insert_one(
        {
            "user_id": user_id,
            "ts": bot.now(),
            "updated_at": bot.now(),
            "text": message,
            "message_type": "weekly_summary",
            "phase": "weekly",
            "trigger": "weekly_summary",
            "related_session_id": None,
            "parse_mode": None,
            "scenario": "weekly_summary",
            "delivered_to_telegram": False,
        }
    )
    bot.log_structured(
        "weekly_summary_sent",
        user_id=user_id,
        days_active=facts.get("days_active"),
        main_blocker=facts.get("main_blocker_pattern"),
        what_worked=facts.get("what_worked"),
    )
    bot.log_event(user_id, "insight", facts)
    bot.set_memory(user_id, "last_weekly_summary", facts, confidence=0.8)


class BrobotScenarioTests(unittest.IsolatedAsyncioTestCase):
    USER_ID_BASE = 980_295_000

    @classmethod
    def tearDownClass(cls):
        bot.clear_test_mode()
        bot.set_test_clock(None)

    async def asyncTearDown(self):
        bot.clear_test_mode()
        bot.set_test_clock(None)

    async def _run_scenario(self, scenario_name: str, *, user_id: int):
        cfg = dev_scenarios.SCENARIO_DEFS[scenario_name]
        bot.clear_test_outbox(user_id)
        bot.set_test_clock(cfg["clock"])
        bot.set_test_mode(suppress_telegram=True, scenario=scenario_name, user_id=user_id)
        bot.seed_scenario(user_id, scenario_name, reset=True)
        try:
            if scenario_name in bot.DAILY_LOOP_SCENARIOS:
                await bot.run_daily_loop_for_user(bot.tg_app, user_id)
            elif scenario_name in bot.SESSION_TICK_SCENARIOS:
                for session_doc in list(bot.sessions.find({"user_id": user_id, "state": "ACTIVE"})):
                    await bot.run_session_tick_for_doc(bot.tg_app, session_doc)
            elif scenario_name == "weekly_summary":
                _run_weekly_summary(user_id)
            return {
                "ops_summary_24h": bot.ops_summary_payload(24, user_id=user_id),
                "test_outbox": bot.get_test_outbox(user_id),
            }
        finally:
            bot.clear_test_mode()


def _make_scenario_test(scenario_name: str, offset: int):
    async def test_method(self):
        user_id = self.USER_ID_BASE + offset
        result = await self._run_scenario(scenario_name, user_id=user_id)
        evaluation = dev_scenarios.evaluate_scenario(scenario_name, result)
        if not evaluation["passed"]:
            self.fail(
                f"{scenario_name} failed: {'; '.join(evaluation['reasons'])}\n"
                f"messages={evaluation['message_types']}\n"
                f"summary={result['ops_summary_24h']}"
            )

    test_method.__name__ = f"test_scenario_{scenario_name}"
    return test_method


for _idx, _scenario_name in enumerate(dev_scenarios.SCENARIO_DEFS):
    setattr(BrobotScenarioTests, f"test_scenario_{_scenario_name}", _make_scenario_test(_scenario_name, _idx))


class BrobotControlLogicTests(unittest.TestCase):
    USER_ID_BASE = 981_295_000

    @classmethod
    def tearDownClass(cls):
        bot.clear_test_mode()
        bot.set_test_clock(None)

    def tearDown(self):
        bot.clear_test_mode()
        bot.set_test_clock(None)

    def _fresh_user(self, offset: int, *, timezone: str = "America/Toronto") -> int:
        user_id = self.USER_ID_BASE + offset
        bot.reset_user_test_data(user_id)
        bot.seed_test_user(user_id, timezone=timezone)
        return user_id

    def test_learned_pressure_stays_within_allowed_band_and_can_win_with_evidence(self):
        user_id = self._fresh_user(1)
        bot.set_profile_fields(user_id, blockers=["tired"])
        context = {
            "trigger": "no_response_after_morning_prompt",
            "blocker": "tired",
            "phase": "intervention",
            "message_type": "intervention",
        }

        for _ in range(5):
            bot.record_outcome(
                user_id,
                {
                    "outcome_type": "proactive_sent",
                    "message_type": "intervention",
                    "phase": "intervention",
                    "trigger": "no_response_after_morning_prompt",
                    "pressure_level": "low",
                },
            )
            bot.record_outcome(
                user_id,
                {
                    "outcome_type": "session_completed",
                    "message_type": "intervention",
                    "phase": "intervention",
                    "trigger": "no_response_after_morning_prompt",
                    "pressure_level": "low",
                    "session_completed": True,
                    "progress_occurred": True,
                },
            )

        chosen = bot.choose_pressure_level(user_id, context)
        self.assertEqual(chosen, "low")
        self.assertIn(chosen, {"low", "medium"})

    def test_precision_reentry_turns_off_after_meaningful_positive_return(self):
        user_id = self._fresh_user(2)
        for minute_offset in (60, 30):
            bot.record_outcome(
                user_id,
                {
                    "outcome_type": "message_skip",
                    "message_type": "intervention",
                    "phase": "intervention",
                    "ts": bot.now() - bot.timedelta(minutes=minute_offset),
                },
            )
        state_before = bot.precision_reentry_state(user_id)
        self.assertTrue(state_before["active"])

        bot.record_outcome(
            user_id,
            {
                "outcome_type": "session_started",
                "message_type": "intervention",
                "phase": "intervention",
                "ts": bot.now() - bot.timedelta(minutes=5),
                "session_started": True,
            },
        )
        state_after = bot.precision_reentry_state(user_id)
        self.assertFalse(state_after["active"])
        self.assertTrue(state_after["recovered_after_silence"])

    def test_timing_selection_prefers_learned_hour_when_evidence_is_strong(self):
        user_id = self._fresh_user(3)
        bot.set_profile_fields(user_id, loop_anchor_hour=8)
        bot.set_memory(user_id, "time_of_day_activity", {"9": 5, "8": 1}, 0.8)
        bot.set_memory(user_id, "time_of_day_slumps", {"8": 4}, 0.8)
        bot.update_control_stat(user_id, "timing_hour", "morning:9", attempts_delta=5, successes_delta=4, weighted_delta=2.8, mark_used=True, mark_success=True)
        bot.update_control_stat(user_id, "timing_hour", "morning:8", attempts_delta=5, successes_delta=0, weighted_delta=-1.3, mark_used=True, mark_success=False)

        hours = bot.daily_loop_hours_for_user(user_id)
        self.assertEqual(hours["morning"], 9)

    def test_low_yield_burst_skips_proactive_messages(self):
        user_id = self._fresh_user(4)
        for minute_offset in (90, 60, 30):
            bot.record_outcome(
                user_id,
                {
                    "outcome_type": "proactive_sent",
                    "message_type": "intervention",
                    "phase": "intervention",
                    "ts": bot.now() - bot.timedelta(minutes=minute_offset),
                },
            )
        decision = bot.should_send_message(user_id, "intervention", {"phase": "intervention"})
        self.assertEqual(decision["decision"], "skip")
        self.assertEqual(decision["reason"], "low_yield_burst")

    def test_repeated_avoidance_escalates_to_clarity_under_severe_goal_decay(self):
        user_id = self._fresh_user(5)
        bot.goals.update_one(
            {"user_id": user_id, "goal": "optimization-of-brobot"},
            {"$set": {"updated_at": bot.now() - bot.timedelta(days=8), "status": "active"}},
        )
        for days_ago, status in enumerate(["blocked", "missed", "partial", "blocked"]):
            date_key = bot.date_key_for_user(user_id, -days_ago)
            bot.daily_intentions.update_one(
                {"user_id": user_id, "date": date_key},
                {"$set": {
                    "user_id": user_id,
                    "date": date_key,
                    "timezone": bot.get_user_timezone(user_id),
                    "selected_goal": "optimization-of-brobot",
                    "target": f"dragging-target-{days_ago}",
                    "fallback": "reduce to one visible move",
                    "status": status,
                    "created_at": bot.now() - bot.timedelta(days=days_ago),
                    "updated_at": bot.now() - bot.timedelta(days=days_ago),
                }},
                upsert=True,
            )
        for idx in range(3):
            bot.logs.insert_one(
                {
                    "user_id": user_id,
                    "ts": bot.now() - bot.timedelta(hours=idx + 1),
                    "kind": "loop_status",
                    "data": {"phase": "midday", "status": "avoiding"},
                }
            )

        intervention = bot.choose_intervention(user_id, "repeated_avoidance")
        self.assertEqual(intervention["mode"], "clarity")
        self.assertIn(intervention["action_offer"], {"replace_goal", "split_goal", "shrink_target", "next_visible_win"})


if __name__ == "__main__":
    unittest.main()
