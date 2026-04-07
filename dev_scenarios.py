import argparse
import json
import os
import sys

import requests

SCENARIO_DEFS = {
    "fresh_morning": {
        "clock": "2026-04-07T08:05:00-04:00",
        "expectation": "Expect a morning anchor prompt and daily_loop.morning_anchor activity.",
        "expected_messages": {"morning_prompt"},
        "suites": {"core", "daily_loop", "live_full"},
    },
    "midday_active": {
        "clock": "2026-04-07T13:05:00-04:00",
        "expectation": "Expect the midday check prompt and prompt_delivery.daily_loop.midday to rise.",
        "expected_messages": {"midday_prompt"},
        "suites": {"core", "daily_loop", "live_full"},
    },
    "missed_day_recovery": {
        "clock": "2026-04-07T08:10:00-04:00",
        "expectation": "Expect missed-day recovery instead of a standard morning prompt.",
        "expected_messages": {"intervention"},
        "suites": {"core", "recovery", "live_full"},
    },
    "repeated_avoidance": {
        "clock": "2026-04-07T14:15:00-04:00",
        "expectation": "Expect a recovery intervention and control/intervention activity.",
        "expected_messages": {"intervention"},
        "suites": {"core", "recovery", "pressure", "live_full"},
    },
    "stale_goal": {
        "clock": "2026-04-07T05:00:00-04:00",
        "expectation": "Expect stale-goal or goal-decay intervention output.",
        "expected_messages": {"intervention"},
        "suites": {"core", "recovery", "live_full"},
    },
    "blocked_focus": {
        "clock": "2026-04-07T17:30:00-04:00",
        "expectation": "Expect a focus completion prompt from sessions tick.",
        "expected_messages": {"session_completion"},
        "suites": {"core", "focus", "live_full"},
    },
    "focus_nudge_start": {
        "clock": "2026-04-07T15:20:00-04:00",
        "expectation": "Expect a focus start nudge asking whether the session began.",
        "expected_messages": {"session_nudge"},
        "suites": {"focus", "live_full"},
    },
    "focus_nudge_mid_session": {
        "clock": "2026-04-07T15:35:00-04:00",
        "expectation": "Expect a mid-session nudge asking if the user is still working.",
        "expected_messages": {"session_nudge"},
        "suites": {"focus", "live_full"},
    },
    "weekly_summary": {
        "clock": "2026-04-13T09:00:00-04:00",
        "expectation": "Expect a weekly summary message and insight/control stats updates.",
        "expected_messages": {"weekly_summary"},
        "summary_checks": [{"path": ("intervention_outcomes", "total"), "min": 1}],
        "suites": {"core", "weekly", "live_full"},
    },
    "onboarding_dropoff": {
        "clock": "2026-04-07T12:00:00-04:00",
        "expectation": "Expect onboarding drop-off counts to appear in ops summary.",
        "expected_messages": set(),
        "summary_checks": [{"path": ("onboarding", "dropoff_24h"), "min": 1}],
        "suites": {"core", "onboarding", "live_full"},
    },
    "onboarding_manual_timezone": {
        "clock": "2026-04-07T12:00:00-04:00",
        "expectation": "Expect an incomplete onboarding profile waiting for manual timezone input.",
        "expected_messages": set(),
        "summary_checks": [{"path": ("onboarding", "incomplete_profiles"), "min": 1}],
        "suites": {"onboarding", "live_full"},
    },
    "morning_followup_tired": {
        "clock": "2026-04-07T11:30:00-04:00",
        "expectation": "Expect a tired-user follow-up intervention after no response to the morning prompt.",
        "expected_messages": {"intervention"},
        "suites": {"recovery", "pressure", "live_full"},
    },
    "anxious_restart": {
        "clock": "2026-04-07T08:12:00-04:00",
        "expectation": "Expect a gentler missed-day restart intervention for an anxious user.",
        "expected_messages": {"intervention"},
        "suites": {"recovery", "pressure", "live_full"},
    },
    "active_session_shield": {
        "clock": "2026-04-07T13:15:00-04:00",
        "expectation": "Expect no extra prompt because an active focus session should defer the daily-loop send.",
        "expected_messages": set(),
        "summary_checks": [{"path": ("control", "events", "message_defer"), "min": 1}],
        "suites": {"timing", "focus", "live_full"},
    },
    "low_yield_burst": {
        "clock": "2026-04-07T13:20:00-04:00",
        "expectation": "Expect no prompt because recent low-yield sends should trigger burst skipping.",
        "expected_messages": set(),
        "summary_checks": [{"path": ("control", "events", "message_skip"), "min": 1}],
        "suites": {"timing", "pressure", "live_full"},
    },
    "adaptive_morning_shift": {
        "clock": "2026-04-07T09:05:00-04:00",
        "expectation": "Expect a morning prompt at the learned better hour instead of the default anchor hour.",
        "expected_messages": {"morning_prompt"},
        "suites": {"timing", "adaptive", "live_full"},
    },
    "goal_decay_replace": {
        "clock": "2026-04-07T05:10:00-04:00",
        "expectation": "Expect a goal-decay intervention shaped by repeated friction and stale progress.",
        "expected_messages": {"intervention"},
        "suites": {"adaptive", "recovery", "live_full"},
    },
    "west_coast_morning": {
        "clock": "2026-04-07T11:05:00-04:00",
        "expectation": "Expect the morning prompt to respect a west-coast user's local timezone.",
        "expected_messages": {"morning_prompt"},
        "suites": {"timing", "timezone", "live_full"},
    },
    "evening_wrapup": {
        "clock": "2026-04-07T19:10:00-04:00",
        "expectation": "Expect the end-of-day check prompt for an active intention.",
        "expected_messages": {"eod_prompt"},
        "suites": {"daily_loop", "live_full"},
    },
    "ruthless_avoidance": {
        "clock": "2026-04-07T14:25:00-04:00",
        "expectation": "Expect a sharper avoidance intervention for a ruthless, perfectionist profile.",
        "expected_messages": {"intervention"},
        "suites": {"pressure", "recovery", "live_full"},
    },
}

DEFAULT_SCENARIOS = [
    "fresh_morning",
    "midday_active",
    "missed_day_recovery",
    "repeated_avoidance",
    "stale_goal",
    "blocked_focus",
    "weekly_summary",
    "onboarding_dropoff",
]


def list_suite_names() -> list[str]:
    names = set()
    for cfg in SCENARIO_DEFS.values():
        names.update(cfg.get("suites", set()))
    return sorted(names)


def scenarios_for_suite(name: str) -> list[str]:
    return [scenario for scenario, cfg in SCENARIO_DEFS.items() if name in cfg.get("suites", set())]


def post(base_url: str, path: str, secret: str, payload: dict, *, timeout: int = 180):
    url = f"{base_url.rstrip('/')}{path}?secret={secret}"
    resp = requests.post(url, json=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def get(base_url: str, path: str, secret: str, *, timeout: int = 180):
    url = f"{base_url.rstrip('/')}{path}?secret={secret}"
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def get_summary(base_url: str, secret: str, hours: int, *, timeout: int = 180):
    url = f"{base_url.rstrip('/')}/ops/summary?secret={secret}&hours={hours}"
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def set_clock(base_url: str, secret: str, iso_value: str | None = None, *, reset: bool = False, timeout: int = 180):
    payload = {"reset": bool(reset)}
    if iso_value:
        payload["iso"] = iso_value
    return post(base_url, "/dev/clock", secret, payload, timeout=timeout)


def run_scenario(base_url: str, secret: str, user_id: int, scenario: str, *, reset: bool = True, suppress_telegram: bool = True, timeout: int = 180):
    return post(
        base_url,
        "/dev/scenarios/run",
        secret,
        {"user_id": user_id, "scenario": scenario, "reset": reset, "suppress_telegram": suppress_telegram},
        timeout=timeout,
    )


def compact_ops(summary: dict) -> dict:
    return {
        "prompt_delivery": summary.get("prompt_delivery", {}),
        "user_responses": summary.get("user_responses", {}),
        "intervention_outcomes": summary.get("intervention_outcomes", {}),
        "control": summary.get("control", {}),
        "sessions": summary.get("sessions", {}),
        "onboarding": summary.get("onboarding", {}),
    }


def _value_at_path(payload: dict, path: tuple[str, ...]):
    current = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def evaluate_scenario(name: str, result: dict) -> dict:
    cfg = SCENARIO_DEFS[name]
    summary = result.get("ops_summary_24h", {}) or {}
    outbox = result.get("test_outbox", []) or []
    message_types = {item.get("message_type") for item in outbox if item.get("message_type")}
    reasons = []
    passed = True
    expected_messages = cfg.get("expected_messages", set()) or set()

    if expected_messages and not (message_types & expected_messages):
        passed = False
        reasons.append(f"expected one of {sorted(expected_messages)} in captured messages, got {sorted(message_types)}")

    for check in cfg.get("summary_checks", []):
        value = _value_at_path(summary, tuple(check["path"]))
        if "min" in check and int(value or 0) < int(check["min"]):
            passed = False
            reasons.append(f"expected {'.'.join(check['path'])} >= {check['min']}, got {value!r}")
        if "equals" in check and value != check["equals"]:
            passed = False
            reasons.append(f"expected {'.'.join(check['path'])} == {check['equals']!r}, got {value!r}")

    return {
        "passed": passed,
        "reasons": reasons or ["ok"],
        "message_types": sorted(message_types),
    }


def print_section(title: str, payload):
    print(title)
    print(json.dumps(payload, indent=2, default=str))


def collect_scenarios(args) -> list[str]:
    if args.scenario:
        return [args.scenario]
    if args.suite:
        return scenarios_for_suite(args.suite)
    if args.all:
        return list(DEFAULT_SCENARIOS)
    return []


def run_many(args, scenarios: list[str]):
    results = []
    for idx, scenario in enumerate(scenarios):
        cfg = SCENARIO_DEFS[scenario]
        scenario_user_id = int(args.user_id) if args.live else int(args.user_id) + idx
        clock_value = cfg.get("clock")
        print(f"Running scenario: {scenario} (user {scenario_user_id})")
        if clock_value:
            clock_payload = set_clock(args.base_url, args.secret, clock_value, timeout=args.timeout)
        else:
            clock_payload = None
        scenario_result = run_scenario(
            args.base_url,
            args.secret,
            scenario_user_id,
            scenario,
            reset=True,
            suppress_telegram=not args.live,
            timeout=args.timeout,
        )
        summary = compact_ops(scenario_result.get("ops_summary_24h", {}))
        results.append(
            {
                "scenario": scenario,
                "user_id": scenario_user_id,
                "clock": clock_payload,
                "expectation": cfg.get("expectation"),
                "summary": summary,
                "evaluation": evaluate_scenario(scenario, scenario_result),
                "outbox": scenario_result.get("test_outbox", []),
            }
        )
    return results


def print_results(title: str, results: list[dict], live: bool):
    print(title)
    for item in results:
        print(f"\n=== {item['scenario']} ===")
        print(f"User: {item['user_id']}")
        if item.get("clock"):
            print(f"Clock: {item['clock'].get('fake_utc_now')}")
        print(f"Mode: {'live' if live else 'suppressed'}")
        print(f"Expectation: {item['expectation']}")
        print(f"Result: {'PASS' if item['evaluation']['passed'] else 'FAIL'}")
        print(f"Why: {', '.join(item['evaluation']['reasons'])}")
        print(f"Captured message types: {item['evaluation']['message_types']}")
        print(json.dumps(item["summary"], indent=2, default=str))


def print_catalog():
    print("Available scenarios:")
    for name, cfg in SCENARIO_DEFS.items():
        suites = ", ".join(sorted(cfg.get("suites", set())))
        print(f"- {name}")
        print(f"  suites: {suites}")
        print(f"  expectation: {cfg.get('expectation')}")
    print("\nAvailable suites:")
    for suite in list_suite_names():
        scenarios = ", ".join(scenarios_for_suite(suite))
        print(f"- {suite}: {scenarios}")


def main():
    parser = argparse.ArgumentParser(description="Brobot expanded dev scenario runner")
    parser.add_argument("--base-url", default=os.getenv("BROBOT_BASE_URL", "http://127.0.0.1:10000"))
    parser.add_argument("--secret", default=os.getenv("CRON_SECRET"))
    parser.add_argument("--user-id", type=int, default=810295446)
    parser.add_argument("--clock", help="ISO datetime for fake clock, for example 2026-04-07T08:05:00-04:00")
    parser.add_argument("--reset-clock", action="store_true")
    parser.add_argument("--scenario", choices=sorted(SCENARIO_DEFS), help="Scenario to seed and run")
    parser.add_argument("--suite", choices=list_suite_names(), help="Run a themed suite of scenarios")
    parser.add_argument("--all", action="store_true", help="Run the default core scenario set")
    parser.add_argument("--list", action="store_true", help="List all scenarios and suites")
    parser.add_argument("--live", action="store_true", help="Send scenario messages to your Telegram chat instead of suppressing them")
    parser.add_argument("--summary-hours", type=int, default=24)
    parser.add_argument("--timeout", type=int, default=180, help="Per-request HTTP timeout in seconds")
    args = parser.parse_args()

    if args.list:
        print_catalog()
        return

    if not args.secret:
        parser.error("--secret is required unless you are using --list")

    if args.reset_clock or args.clock:
        data = set_clock(args.base_url, args.secret, args.clock, reset=bool(args.reset_clock), timeout=args.timeout)
        print_section("Clock:", data)

    scenarios = collect_scenarios(args)
    if scenarios:
        results = run_many(args, scenarios)
        title = "All scenarios summary:" if args.all else f"Suite summary: {args.suite}" if args.suite else "Scenario result:"
        print_results(title, results, args.live)
        if len(results) == 1:
            item = results[0]
            print(f"\nExpectation: {item['expectation']}")
            print(f"Result: {'PASS' if item['evaluation']['passed'] else 'FAIL'}")
            print(f"Why: {', '.join(item['evaluation']['reasons'])}")
        summary = get_summary(args.base_url, args.secret, args.summary_hours, timeout=args.timeout)
        print_section("\nFinal ops summary:" if len(results) > 1 else "Ops summary:", summary)
        return

    summary = get_summary(args.base_url, args.secret, args.summary_hours, timeout=args.timeout)
    print_section("Ops summary:", summary)


if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as exc:
        print(exc.response.text, file=sys.stderr)
        raise
