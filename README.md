# DS Jobs Bot — Entry-Level (US) • Slack

A Slack bot that aggregates **data roles** (Data Analyst, Data Engineer, Data Scientist, Analytics Engineer, ML Engineer, BI/Analytics) from multiple **ATS/feeds**, then filters to **US-only** and **≤ 3 years experience** (i.e., “less than 4”). It **prioritizes sponsorship-friendly** roles (H-1B / “visa sponsorship” signals) and posts a single **grouped list** to your Slack channel **every 30 minutes** via GitHub Actions.

> Designed for immigrants and early-career applicants who want **fresh, relevant** US data jobs—no clearance, no senior titles, newest first.

---

## What it does

- **Collects jobs** from public, stable endpoints:
  - ATS: **Greenhouse, Lever, Ashby, SmartRecruiters, Workable, Recruitee, BambooHR, Personio**
  - RSS: **AI-Jobs**, **We Work Remotely (Data)**, **Jobspresso**
  - Optional aggregators (keys required): **Adzuna**, **USAJOBS**
- **Filters aggressively**
  - Data roles only (title/description gate + hard-skill boosters)
  - **US-only** (explicit Canada exclusion)
  - **≤ 3 years exp** (blocks 4+ yrs and senior/staff/principal/lead/manager/architect/director)
  - Blocks clearance (TS/SCI, Public Trust, etc.)
  - Optional: exclude internships
- **Ranks results**
  - **Newest first**, slight boost for **sponsorship-friendly** language
- **Posts to Slack as one tidy message**
  - Grouped by category (Data Engineering / Data Science & ML / Analytics (BI/Product) / etc.)
  - Shows **title**, **company**, **location**, **posted time (UTC)**, **source**, and **link**
  - Adds 🔥 tag for sponsor-friendly roles
- **Remembers what it already posted**
  - Only **new** jobs are posted each run; falls back to a small snapshot if nothing new
- **Handles Slack limits** (backs off on 429; uploads CSV if the message would be too long)
- **Artifacts**: Every run saves a CSV and a preview (`out/preview.md`, `out/preview.csv`)

---

## Quick start (GitHub Actions — runs every 30 min)

1. **Add repository secrets** (Settings → Secrets and variables → Actions → New repository secret):
   - `SLACK_BOT_TOKEN` = your bot token (`xoxb-…`)
   - `SLACK_CHANNEL` = `#your-channel` **(public)** or **Channel ID** like `C09K…` **(private)**
   - *(Optional)* `ADZUNA_APP_ID`, `ADZUNA_APP_KEY`, `USAJOBS_API_KEY`, `USAJOBS_EMAIL`
2. *(Optional)* **Repository variables** (Settings → Secrets and variables → Actions → **Variables**):
   - `RECENCY_DAYS=31`, `MAX_ITEMS_PER_RUN=60`, `ALLOW_INTERNSHIPS=false`, `SNAPSHOT_N=25`
3. **Invite the bot** in Slack: open your channel → `/invite @DS JOBS BOT`
4. **Enable & test**: Repo **Actions** tab → select **ds-job-bot** → **Run workflow**
5. The workflow runs automatically every **30 minutes (UTC)**  
   (cron is in `.github/workflows/cron.yml` → `*/30 * * * *`)

> **Private channels:** Slack requires `SLACK_CHANNEL` to be the **Channel ID** (not `#name`). Open channel → name → **About** → **Channel ID**.

---

## Local run (optional)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# edit .env with your Slack token + channel

python main.py
