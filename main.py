import os, csv, yaml, sqlite3, datetime, io, time, random, re
from datetime import timezone
from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from sources import fetch_all
from filters import (
    is_datasci, violates_clearance, meets_experience_max,
    is_within_days, sponsorship_score, is_us_job,
    title_includes_required, title_level_is_ok, parse_when
)
from taxonomy import categorize
from utils import normalize_url

load_dotenv()

SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")
SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL", "#data_science_jobs_updates")
MAX_MESSAGE_CHARS = int(os.environ.get("MAX_MESSAGE_CHARS", "39000"))
MAX_ITEMS_PER_RUN = int(os.environ.get("MAX_ITEMS_PER_RUN", "60"))
RECENCY_DAYS = int(os.environ.get("RECENCY_DAYS", "31"))
ALLOW_INTERNSHIPS = os.environ.get("ALLOW_INTERNSHIPS", "false").lower() == "true"
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"
SNAPSHOT_N = int(os.environ.get("SNAPSHOT_N", "25"))

client = WebClient(token=SLACK_BOT_TOKEN) if SLACK_BOT_TOKEN else None

DB_PATH = "db.sqlite3"
CSV_DIR = "out"

def now_utc(): return datetime.datetime.now(timezone.utc)

def init():
    os.makedirs(CSV_DIR, exist_ok=True)
    con = sqlite3.connect(DB_PATH); cur = con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY, url TEXT, title TEXT, company TEXT, location TEXT,
        source TEXT, posted_at TEXT, description TEXT, first_seen_at TEXT)""")
    con.commit(); con.close()

def export_csv(jobs):
    ts = now_utc().strftime("%Y%m%d_%H%M%S")
    path = f"{CSV_DIR}/jobs_{ts}.csv"
    cols = ["id","title","company","location","url","source","posted_at","first_seen_at","description"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols); w.writeheader()
        for j in jobs:
            w.writerow({
                "id": j.get("id"), "title": j.get("title"), "company": j.get("company"),
                "location": j.get("location"), "url": j.get("url"),
                "source": j.get("source"), "posted_at": j.get("posted_at"),
                "first_seen_at": now_utc().isoformat(),
                "description": (j.get("description") or "").replace("\n"," ")[:5000],
            })
    print(f"CSV exported: {path}")

def _sleep_with_jitter(seconds): time.sleep(max(0, seconds) + random.uniform(0.25,0.75))
def _post_with_backoff(fn, *args, **kwargs):
    if DRY_RUN: return {"ok": True, "ts": str(time.time())}
    attempts = 0
    while True:
        try:
            return fn(*args, **kwargs)
        except SlackApiError as e:
            attempts += 1
            status = getattr(e.response, "status_code", None)
            if status == 429 and attempts <= 3:
                retry_after = int(e.response.headers.get("Retry-After","1"))
                _sleep_with_jitter(retry_after); continue
            print("Slack error:", getattr(e.response, "data", e)); break

def grouped_message(jobs):
    sections = {}
    for j in jobs:
        sections.setdefault(categorize(j.get("title") or ""), []).append(j)
    lines = ["*Entry-level & < 4 yrs — US-only — newest first (this run)*"]
    idx = 1
    for cat in sorted(sections.keys()):
        arr = sections[cat]
        lines.append(f"\n*{cat}*  _({len(arr)})_")
        for j in arr:
            title = j.get("title") or "Untitled"
            comp  = (j.get("company") or "").strip()
            loc   = j.get("location") or "Location N/A"
            url   = j.get("url") or ""
            dt = parse_when(j.get("posted_at"))
            when_s = "date n/a" if not dt else dt.strftime("%Y-%m-%d %H:%M UTC")
            src   = j.get("source") or ""
            blob = f"{j.get('title','')} {j.get('description','')}".lower()
            tag = " 🔥 *Sponsor-friendly*" if any(x in blob for x in ["h1b","h-1b","sponsor","visa sponsorship","work visa"]) else ""
            lines.append(f"{idx}. *{title}* — {comp} ({loc}) — _{when_s}_  <{url}|link>  · {src}{tag}")
            idx += 1
    return "\n".join(lines)

def write_local_snapshot(jobs):
    os.makedirs("out", exist_ok=True)
    with open("out/preview.md","w",encoding="utf-8") as f:
        f.write(grouped_message(jobs))
    cols = ["title","company","location","url","source","posted_at"]
    with open("out/preview.csv","w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols); w.writeheader()
        for j in jobs:
            w.writerow({k: j.get(k,"") for k in cols})
    print("Wrote snapshot: out/preview.md and out/preview.csv")

def post_to_slack(jobs):
    text = grouped_message(jobs)
    if client:
        if len(text) <= MAX_MESSAGE_CHARS:
            _post_with_backoff(client.chat_postMessage,
                               channel=SLACK_CHANNEL, text=text,
                               unfurl_links=False, unfurl_media=False)
        else:
            buf = io.StringIO()
            w = csv.DictWriter(buf, fieldnames=["title","company","location","url","source","posted_at"])
            w.writeheader()
            for j in jobs: w.writerow({k: j.get(k,"") for k in w.fieldnames})
            _post_with_backoff(client.files_upload_v2,
                               channel=SLACK_CHANNEL,
                               filename="jobs.csv",
                               content=buf.getvalue(),
                               title="Entry-level & <4 yrs jobs (latest run)",
                               initial_comment="Message was long — attached CSV with this run's jobs.")
    else:
        write_local_snapshot(jobs)

def main():
    with open("config.yaml","r",encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    include_kw = (cfg.get("keywords") or {}).get("include", [])
    exclude_kw = (cfg.get("keywords") or {}).get("exclude", [])
    title_must = cfg.get("title_must_include", [])
    title_block = cfg.get("title_level_blocklist", [])
    clr_block = cfg.get("clearance_blocklist", [])
    max_years = cfg.get("experience", {}).get("max_required_years", 3)
    enforce_us = (cfg.get("us_filter") or {}).get("enforce", True)

    init()

    print("Fetching…")
    raw = fetch_all(cfg) or []
    print(f"Fetched {len(raw)} jobs")

    filtered = []
    for j in raw:
        if not j.get("id") or not j.get("url") or not j.get("title"):
            continue
        j["url"] = normalize_url(j["url"])
        if not is_within_days(j.get("posted_at"), days=RECENCY_DAYS):  continue
        if not is_datasci(j, include_kw, exclude_kw):                 continue
        if title_must and not title_includes_required(j.get("title"), title_must):  continue
        if not title_level_is_ok(j.get("title"), title_block):        continue
        if violates_clearance(j, clr_block):                          continue
        if not meets_experience_max(j, max_years=max_years):          continue
        if not ALLOW_INTERNSHIPS:
            tl = (j.get("title") or "").lower()
            if re.search(r"\bintern(ship)?\b|\bco-?op\b", tl):        continue
        if enforce_us and not is_us_job(j):                           continue
        filtered.append(j)

    # Rank newest first + tiny sponsorship boost
    def posted_ts(job):
        dt = parse_when(job.get("posted_at"))
        try: return dt.timestamp() if dt else 0.0
        except: return 0.0

    ranked = []
    for j in filtered:
        ts = posted_ts(j)
        s  = sponsorship_score(j, cfg)
        ranked.append((ts, s, j))
    ranked.sort(key=lambda x: (x[0], x[1]), reverse=True)
    to_post = [j for *_ignore, j in ranked][:MAX_ITEMS_PER_RUN]

    export_csv([j for *_ignore, j in ranked])

    if to_post:
        post_to_slack(to_post)
        print(f"Ready to post this run: {len(to_post)}")
    else:
        snap = [j for *_ignore, j in ranked][:max(1, SNAPSHOT_N)]
        if snap:
            post_to_slack(snap)
            print(f"No new jobs — wrote snapshot of {len(snap)}")
        else:
            print("No jobs after filters — consider widening RECENCY_DAYS.")

if __name__ == "__main__":
    main()
