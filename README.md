# Brobot v2

Brobot v2 is a Telegram accountability bot focused on friction reduction.
It uses button-first conversational flows for onboarding, daily intentions, daily loop prompts, focus sessions, and recovery nudges.

## What It Does

- Guides onboarding with buttons plus a few text answers:
  - timezone
  - 1-3 active goals
  - why each goal matters
  - push style
  - work start time
  - common blockers
  - restart size
- Stores a daily intention with:
  - date
  - selected goal
  - target
  - fallback
  - status
- Runs a daily loop:
  - Morning Anchor: `Continue yesterday / New target / You choose`
  - Midday Check: `Started / Almost / Avoiding`
  - End-of-Day: `Done / Partial / Missed / Reset tomorrow`
- Starts focus sessions from chat buttons or `/focus`
- Sends optional session nudges and completion prompts
- Uses a deterministic intervention engine for:
  - no response after morning prompt
  - inactivity after target selection
  - unfinished session
  - repeated avoidance
  - missed day
  - stale goal
- Stores structured behavioral memory and intervention outcomes
- Sends a weekly summary based on stored facts, with AI only phrasing the output

## Primary Commands

Primary command surface:

- `/start`
- `/goals`
- `/focus`
- `/override`
- `/settings`

The bot is intentionally button-first now. Setup, daily intention, timing, and goal management should happen through `/start`, `/settings`, `/goals`, and inline buttons rather than older slash-command setup flows.

## Architecture Notes

- Runtime shape:
  - FastAPI app receives Telegram webhooks
  - `python-telegram-bot` handles commands, callbacks, and text routing
  - MongoDB stores users, goals, sessions, state, intentions, memory, and intervention outcomes
  - Cohere is used for phrasing only, not business logic
- Main app entrypoint:
  - `Telegram_Bot.py`
- Deterministic logic decides:
  - current goal
  - daily loop timing
  - trigger detection
  - blocker mapping
  - intervention mode/action
  - weekly summary facts

## Cron Endpoints

Protected by `?secret=$CRON_SECRET`:

- `GET /cron/daily`
  - Runs the daily loop service
  - Sends morning/midday/end-of-day prompts
  - Sends inactivity/avoidance/missed-day/stale-goal recovery prompts
- `GET /cron/weekly`
  - Sends weekly summaries
- `GET /cron/sessions-tick`
  - Sends focus-session nudges
  - Sends completion prompts when sessions time out

Other useful endpoints:

- `GET /health`
- `GET /ops/summary?secret=...`
- `GET /ops/verify?secret=...`
- `GET /dev/clock?secret=...`
- `POST /dev/clock?secret=...`
- `POST /dev/scenarios/seed?secret=...`
- `POST /dev/scenarios/run?secret=...`
- `POST /dev/outcomes/record?secret=...`
- `POST /webhook`
- `POST /sessions/start`
- `POST /sessions/finish`
- `POST /events`

## Environment Variables

Required:

- `BOT_TOKEN`
- `MONGO_URI`
- `COHERE_API_KEY`

Recommended:

- `COHERE_MODEL`
  - default: `command-r-08-2024`
- `TZ`
  - default: `America/Toronto`
- `TELEGRAM_SECRET_TOKEN`
- `CRON_SECRET`
- `WEBHOOK_URL`
  - public webhook base URL, for example `https://brobot-l2g7.onrender.com`
- `LOG_LEVEL`
  - default: `INFO`

## Local Run

1. Create and activate a Python 3.11 environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Set environment variables.
4. Run the app:

```bash
uvicorn Telegram_Bot:app --host 0.0.0.0 --port 10000
```

5. For local testing without Telegram traffic, verify:

```bash
python -m py_compile Telegram_Bot.py
```

## Render Deployment Notes

- The web service runs the FastAPI app from `Telegram_Bot.py`.
- `WEBHOOK_URL` should match the public Render hostname.
- Render cron jobs should call:
  - `/cron/daily`
  - `/cron/weekly`
  - `/cron/sessions-tick`
- The current `render.yaml` is aligned to:
  - `https://brobot-l2g7.onrender.com`

## Operational Checks

After deploy:

1. Open `/health` and confirm it returns `ok`.
2. Open `/ops/verify?secret=...` and confirm:
   - Mongo is healthy
   - webhook URL matches `/webhook`
   - pending updates and last webhook error look sane
3. Send `/start` and verify onboarding/settings buttons appear.
4. Create a daily intention and start a focus session from buttons.
5. Trigger:
   - `/cron/daily`
   - `/cron/weekly`
   - `/cron/sessions-tick`
6. Open `/ops/summary?secret=...` and review:
   - prompt delivery
   - user responses
   - intervention outcomes
   - onboarding drop-off
   - session finish patterns
7. Check logs for structured events such as:
   - `morning_prompt_sent`
   - `midday_prompt_sent`
   - `eod_prompt_sent`
   - `session_nudge_sent`
   - `intervention_send`
   - `weekly_summary_sent`

## Fast Test Harness

You do not need to wait days or weeks to test behavior.

Brobot now supports:

- fake clock override through protected dev endpoints
- scenario seeding for common user states
- direct outcome injection for control-layer testing

### Set a fake clock

```bash
curl -X POST "http://127.0.0.1:10000/dev/clock?secret=$CRON_SECRET" ^
  -H "Content-Type: application/json" ^
  -d "{\"iso\":\"2026-04-07T08:05:00-04:00\"}"
```

Reset it:

```bash
curl -X POST "http://127.0.0.1:10000/dev/clock?secret=$CRON_SECRET" ^
  -H "Content-Type: application/json" ^
  -d "{\"reset\":true}"
```

### Run fast scenarios

Supported scenarios:

- `fresh_morning`
- `midday_active`
- `missed_day_recovery`
- `repeated_avoidance`
- `stale_goal`
- `blocked_focus`
- `focus_nudge_start`
- `focus_nudge_mid_session`
- `weekly_summary`
- `onboarding_dropoff`
- `onboarding_manual_timezone`
- `morning_followup_tired`
- `anxious_restart`
- `active_session_shield`
- `low_yield_burst`
- `adaptive_morning_shift`
- `goal_decay_replace`
- `west_coast_morning`
- `evening_wrapup`
- `ruthless_avoidance`

Example:

```bash
curl -X POST "http://127.0.0.1:10000/dev/scenarios/run?secret=$CRON_SECRET" ^
  -H "Content-Type: application/json" ^
  -d "{\"user_id\":810295446,\"scenario\":\"fresh_morning\",\"reset\":true}"
```

### Scenario runner script

You can also use:

```bash
py -3 dev_scenarios.py --base-url http://127.0.0.1:10000 --secret YOUR_CRON_SECRET --clock 2026-04-07T08:05:00-04:00 --scenario fresh_morning
```

Run all built-in scenarios in one go:

```bash
py -3 dev_scenarios.py --base-url http://127.0.0.1:10000 --secret YOUR_CRON_SECRET --all
```

List all scenarios and themed suites:

```bash
py -3 dev_scenarios.py --list
```

Run a themed suite:

```bash
py -3 dev_scenarios.py --base-url http://127.0.0.1:10000 --secret YOUR_CRON_SECRET --suite pressure
```

To see the proof messages in your Telegram chat instead of suppressing them:

```bash
py -3 dev_scenarios.py --base-url http://127.0.0.1:10000 --secret YOUR_CRON_SECRET --user-id YOUR_TELEGRAM_USER_ID --all --live
```

### Detailed run steps

Local:

1. Open a terminal in the project folder.
2. Install dependencies if needed:

```bash
pip install -r requirements.txt
```

3. Make sure your `.env` has:
   - `BOT_TOKEN`
   - `MONGO_URI`
   - `COHERE_API_KEY`
   - `CRON_SECRET`

4. Start the app:

```bash
uvicorn Telegram_Bot:app --host 0.0.0.0 --port 10000
```

5. In a second terminal, run one scenario:

```bash
py -3 dev_scenarios.py --base-url http://127.0.0.1:10000 --secret YOUR_CRON_SECRET --scenario fresh_morning
```

6. To run all scenarios:

```bash
py -3 dev_scenarios.py --base-url http://127.0.0.1:10000 --secret YOUR_CRON_SECRET --all
```

Render / deployed service:

1. Keep your deployed app live.
2. Use the Render base URL instead of localhost:

```bash
py -3 dev_scenarios.py --base-url https://brobot-l2g7.onrender.com --secret YOUR_CRON_SECRET --scenario fresh_morning
```

3. Or run all:

```bash
py -3 dev_scenarios.py --base-url https://brobot-l2g7.onrender.com --secret YOUR_CRON_SECRET --all
```

### What the script does

- sets a fake clock automatically for the chosen scenario if you do not pass `--clock`
- seeds the user into that scenario
- runs the relevant cron path
- supports larger themed suites such as `focus`, `pressure`, `timing`, `recovery`, `onboarding`, `adaptive`, `timezone`, and `live_full`
- prints:
  - scenario result
  - expectation
  - ops summary snapshot

### Useful manual commands

Set fake time:

```bash
curl -X POST "http://127.0.0.1:10000/dev/clock?secret=$CRON_SECRET" ^
  -H "Content-Type: application/json" ^
  -d "{\"iso\":\"2026-04-07T13:05:00-04:00\"}"
```

Reset fake time:

```bash
curl -X POST "http://127.0.0.1:10000/dev/clock?secret=$CRON_SECRET" ^
  -H "Content-Type: application/json" ^
  -d "{\"reset\":true}"
```

Run a specific scenario by API:

```bash
curl -X POST "http://127.0.0.1:10000/dev/scenarios/run?secret=$CRON_SECRET" ^
  -H "Content-Type: application/json" ^
  -d "{\"user_id\":810295446,\"scenario\":\"blocked_focus\",\"reset\":true}"
```

### What to check

- `fresh_morning`
  - expect a morning anchor prompt
- `midday_active`
  - expect the midday check
- `missed_day_recovery`
  - expect missed-day recovery instead of a normal morning prompt
- `repeated_avoidance`
  - expect recovery / blocker-driven intervention
- `stale_goal`
  - expect stale-goal or goal-decay intervention
- `blocked_focus`
  - expect focus completion prompt from session tick
- `weekly_summary`
  - expect a weekly summary message
- `onboarding_dropoff`
  - inspect `/ops/summary` for onboarding drop-off counts

### Recommended quick full test pass

1. Run:

```bash
py -3 dev_scenarios.py --base-url http://127.0.0.1:10000 --secret YOUR_CRON_SECRET --all
```

2. Watch Telegram and confirm messages arrive for the message-producing scenarios:
   - `fresh_morning`
   - `midday_active`
   - `missed_day_recovery`
   - `repeated_avoidance`
   - `stale_goal`
   - `blocked_focus`
   - `weekly_summary`

3. Open:

```text
http://127.0.0.1:10000/ops/summary?secret=YOUR_CRON_SECRET
```

4. Confirm:
   - `prompt_delivery.daily_loop` is non-empty
   - `intervention_outcomes.total` increases
   - `control.events` contains things like `proactive_sent`, `button_tap`, `session_started`, `session_completed`, `message_deferred`, or `message_skipped`
   - `sessions.finishes` changes after `blocked_focus`
   - `onboarding.dropoff_24h` is visible after `onboarding_dropoff`

### Control-layer verification

After running scenarios, inspect:

- `/ops/summary?secret=...`
  - `control.events`
  - `prompt_delivery`
  - `intervention_outcomes`
- Render/local logs for:
  - `message_suppressed`
  - `morning_prompt_sent`
  - `midday_prompt_sent`
  - `eod_prompt_sent`
  - `session_nudge_sent`
  - `weekly_summary_sent`

## Safety Notes

- Do not commit live secrets.
- Rotate any secrets that were previously committed.
- Keep Mongo timezones consistent and prefer the bot's stored user timezone for user-facing scheduling.
