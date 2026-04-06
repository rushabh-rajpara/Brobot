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

Backward-compatible legacy commands still exist:

- `/setgoal`
- `/setactive`
- `/checkintime`
- `/checkin`
- `/stats`

These older commands are kept for compatibility, but the intended setup flow is now button-first through `/start` and `/settings`.

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

## Safety Notes

- Do not commit live secrets.
- Rotate any secrets that were previously committed.
- Keep Mongo timezones consistent and prefer the bot's stored user timezone for user-facing scheduling.
