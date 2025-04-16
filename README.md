# 🤖 BroBot – Your AI Accountability Partner

**BroBot** is your no-nonsense, always-on, motivational sidekick living inside Telegram.  
It keeps you focused, checks in regularly, tracks your goals, and isn't afraid to drop a roast when you're slacking.

---

## 🌟 Features

### 🧠 AI Motivation Engine (Powered by Cohere)
- Understands your replies and gives smart responses
- Motivational when you're down, savage when you're distracted
- Personalized based on your **current daily goal**

### 🕒 Daily Routine Management
- **9 AM morning motivation** to kickstart your day
- **Goal prompt** every morning to set your main focus
- **2-hour check-ins** to keep you accountable (skips during work hours)
- **10:30 PM reflection** to ask if you completed your goal
- **Midday goal reminder** (1:30 PM) to keep it top-of-mind

### 🔥 Goal Streak + Badges
- Tracks your daily goal streak
- Awards motivational badges based on your consistency:
  - 🥉 Bronze • 🥈 Silver • 🥇 Gold • 🔥 Flame • 🏆 Platinum Crown
- View your streak + badge with `/streak`

### 📈 Weekly Progress & History
- Emoji-based 7-day progress chart (✅ / ❌)
- Full daily goal archive with `/history`

### 👀 Passive Mode Detection
- If you don't set a goal in 5 hours, BroBot checks in:
  > "Yo… you ghosting your goals again?"

### 📅 Smart Scheduling
- Customize job hours with `/schedule`
- BroBot stays silent during your work blocks

### ⏸️ Manual Control
- Pause with `/pause`, resume with `/resume`

### 📊 Usage Stats
- All commands listed with `/help`

---

## 🧰 Tech Stack

- **Python**
- **python-telegram-bot v20+**
- **Cohere API** (for AI replies)
- **MongoDB Atlas** (for memory)
- **APScheduler** (for scheduled tasks)
- **Railway / Render / Replit** (for free deployment)

---

## 📦 Command List

| Command        | Description                                       |
|----------------|---------------------------------------------------|
| `/start`       | Wake up BroBot                                    |
| `/goal`        | Set today's main focus                            |
| `/status`      | View current goal                                 |
| `/pause`       | Pause all check-ins                               |
| `/resume`      | Resume check-ins                                  |
| `/schedule`    | Set your job schedule                             |
| `/showschedule`| Show current job schedule                         |
| `/streak`      | View your streak + motivational badge             |
| `/history`     | See last 7 days of goals                          |                             |
| `/help`        | Show all commands                                 |

---

## 🚀 Getting Started

1. Clone the repo
2. Add your `BOT_TOKEN`, `MONGO_URL`, `CHAT_ID`, and `COHERE_API_KEY`
3. Run the script:
   ```bash
   python File_name.py
