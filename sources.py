import os, requests, feedparser
from datetime import datetime, timezone
from typing import List, Dict
from utils import normalize_url

UA = {"User-Agent": "ds-job-bot/1.0 (+github actions)"}

DEFAULT_SOURCES = {
  "greenhouse_companies": [
    "airbnb","affirm","asana","atlassian","box","brex","cloudflare","coinbase",
    "databricks","datadog","doordash","dropbox","duolingo","figma","hubspot",
    "instacart","mongodb","notion","okta","openai","plaid","reddit","rippling",
    "robinhood","roblox","samsara","shopify","slack","snowflake","stripe","twilio","zapier","zscaler"
  ],
  "lever_companies": ["discord","coursera","elastic","postman","block","scaleai","notion","niantic","cointracker","calm"],
  "ashby_subdomains": ["openai","anthropic","ramp","rippling","retool","linear","vercel","mercury","notion","scaleai","hex","persona","cruise","humane"],
  "smartrecruiters_companies": ["smartrecruiters","ubisoft","nokia"],
  "workable_accounts": ["mozilla","hotjar","komodohealth","canonical"],
  "recruitee_companies": ["trivago","backbase","mollie"],
  "bamboohr_subdomains": ["weightsandbiases","chainalysis"],
  "personio_companies": ["deepset","y42"],
  "rss_feeds": [
    "https://ai-jobs.net/jobfeed/",
    "https://weworkremotely.com/categories/remote-data-jobs.rss",
    "https://jobspresso.co/remote-work-jobs/feed/"
  ],
  "aggregators": {}
}

def _iso_utc(ts: float) -> str:
    try: return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    except: return datetime.now(timezone.utc).isoformat()

# ---- Greenhouse ----
def fetch_greenhouse_company(slug: str) -> List[Dict]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"
    out=[]
    try:
        r = requests.get(url, headers=UA, timeout=25); r.raise_for_status()
        for j in (r.json().get("jobs") or []):
            jid = j.get("id")
            url2 = j.get("absolute_url") or f"https://boards.greenhouse.io/{slug}/jobs/{jid}"
            out.append({
                "id": f"greenhouse:{slug}:{jid}",
                "title": j.get("title"), "company": slug,
                "location": (j.get("location") or {}).get("name"),
                "url": normalize_url(url2),
                "source": "Greenhouse",
                "posted_at": j.get("updated_at") or j.get("created_at"),
                "description": j.get("content") or "",
            })
    except Exception:
        pass
    return out

# ---- Lever ----
def fetch_lever_company(slug: str) -> List[Dict]:
    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
    out=[]
    try:
        r = requests.get(url, headers=UA, timeout=25); r.raise_for_status()
        for j in r.json():
            jid = j.get("id") or j.get("lever_id") or j.get("applyUrl","").split("/")[-1]
            posted = j.get("createdAt")
            if posted and isinstance(posted,(int,float)): posted = _iso_utc(posted/1000)
            url2 = j.get("hostedUrl") or j.get("applyUrl") or f"https://jobs.lever.co/{slug}/{jid}"
            out.append({
                "id": f"lever:{slug}:{jid}",
                "title": j.get("text") or j.get("title"), "company": slug,
                "location": j.get("categories",{}).get("location",""),
                "url": normalize_url(url2),
                "source": "Lever",
                "posted_at": posted,
                "description": j.get("descriptionPlain") or j.get("description") or "",
            })
    except Exception:
        pass
    return out

# ---- Ashby ----
def fetch_ashby(subdomain: str) -> List[Dict]:
    url = f"https://jobs.ashbyhq.com/api/integration/boards/{subdomain}/jobs"
    out=[]
    try:
        r = requests.get(url, headers=UA, timeout=25); r.raise_for_status()
        data = r.json()
        for j in data.get("jobs", []):
            jid = j.get("id") or j.get("slug") or j.get("jobId")
            loc = ", ".join(j.get("locations") or []) or (j.get("location") or "")
            url2 = j.get("jobUrl") or f"https://jobs.ashbyhq.com/{subdomain}/{jid}"
            out.append({
                "id": f"ashby:{subdomain}:{jid}",
                "title": j.get("title"), "company": subdomain,
                "location": loc,
                "url": normalize_url(url2),
                "source": "Ashby",
                "posted_at": j.get("publishedAt") or j.get("createdAt"),
                "description": j.get("descriptionHtml") or j.get("description") or "",
            })
    except Exception:
        pass
    return out

# ---- SmartRecruiters ----
def fetch_smartrecruiters(company: str) -> List[Dict]:
    out=[]; offset=0; limit=100
    try:
        while True:
            url = f"https://api.smartrecruiters.com/v1/companies/{company}/postings?limit={limit}&offset={offset}"
            r = requests.get(url, headers=UA, timeout=25); r.raise_for_status()
            arr = (r.json().get("content") or r.json().get("postings") or [])
            for j in arr:
                jid = j.get("id") or j.get("uuid")
                loc = (j.get("location") or {}).get("city") or (j.get("locationLabel") or "")
                url2 = j.get("ref") or f"https://careers.smartrecruiters.com/{company}/{jid}"
                out.append({
                    "id": f"smartrecruiters:{company}:{jid}",
                    "title": (j.get("name") or j.get("title") or "").strip(),
                    "company": company, "location": loc,
                    "url": normalize_url(url2),
                    "source": "SmartRecruiters",
                    "posted_at": j.get("releasedDate") or j.get("createdOn"),
                    "description": "",
                })
            if len(arr) < limit: break
            offset += limit
    except Exception:
        pass
    return out

# ---- Workable ----
def fetch_workable(account: str) -> List[Dict]:
    out=[]
    try:
        url = f"https://apply.workable.com/api/v3/accounts/{account}/jobs?state=published&limit=100"
        r = requests.get(url, headers=UA, timeout=25); r.raise_for_status()
        for j in (r.json() or {}).get("results", []):
            jid = j.get("shortcode") or j.get("id")
            url2 = f"https://apply.workable.com/{account}/j/{jid}/"
            loc = (j.get("location") or {}).get("location_str") or (j.get("location") or {}).get("city")
            out.append({
                "id": f"workable:{account}:{jid}",
                "title": j.get("title"), "company": account, "location": loc,
                "url": normalize_url(url2),
                "source": "Workable",
                "posted_at": j.get("published_on") or j.get("created_at"),
                "description": "",
            })
    except Exception:
        pass
    return out

# ---- Recruitee ----
def fetch_recruitee(company: str) -> List[Dict]:
    out=[]
    try:
        url = f"https://{company}.recruitee.com/api/offers/?limit=200"
        r = requests.get(url, headers=UA, timeout=25); r.raise_for_status()
        for j in r.json().get("offers", []):
            jid = j.get("id")
            url2 = j.get("careers_url") or f"https://{company}.recruitee.com/o/{j.get('slug')}"
            out.append({
                "id": f"recruitee:{company}:{jid}",
                "title": j.get("title"), "company": company,
                "location": j.get("location") or "",
                "url": normalize_url(url2),
                "source": "Recruitee",
                "posted_at": j.get("created_at"),
                "description": j.get("description") or "",
            })
    except Exception:
        pass
    return out

# ---- BambooHR ----
def fetch_bamboohr(subdomain: str) -> List[Dict]:
    out=[]
    try:
        url = f"https://{subdomain}.bamboohr.com/careers/list"
        r = requests.get(url, headers=UA, timeout=25); r.raise_for_status()
        data = r.json()
        for j in data.get("result", {}).get("jobs", []):
            jid = j.get("id")
            url2 = j.get("jobUrl") or f"https://{subdomain}.bamboohr.com/careers/{jid}"
            out.append({
                "id": f"bamboohr:{subdomain}:{jid}",
                "title": j.get("jobOpeningName") or j.get("jobOpeningNameRaw"),
                "company": subdomain, "location": j.get("location") or j.get("locationCity"),
                "url": normalize_url(url2),
                "source": "BambooHR",
                "posted_at": j.get("datePosted") or j.get("openingDate"),
                "description": "",
            })
    except Exception:
        pass
    return out

# ---- Personio ----
def fetch_personio(company: str) -> List[Dict]:
    out=[]
    for endpoint in [
        f"https://{company}.jobs.personio.de/search.json?language=en",
        f"https://{company}.jobs.personio.de/search.json",
    ]:
        try:
            r = requests.get(endpoint, headers=UA, timeout=25); r.raise_for_status()
            for j in r.json().get("positions", []):
                jid = j.get("id")
                url2 = j.get("url") or f"https://{company}.jobs.personio.de/{jid}"
                out.append({
                    "id": f"personio:{company}:{jid}",
                    "title": j.get("name"), "company": company,
                    "location": j.get("office") or j.get("location") or "",
                    "url": normalize_url(url2),
                    "source": "Personio",
                    "posted_at": j.get("createdAt") or j.get("publishedAt"),
                    "description": j.get("description") or "",
                })
            if out: break
        except Exception:
            continue
    return out

# ---- RSS ----
def fetch_rss(url: str) -> List[Dict]:
    out=[]
    try:
        feed = feedparser.parse(url)
        for e in feed.entries:
            out.append({
                "id": f"rss:{hash(e.get('link'))}",
                "title": e.get("title"), "company": "", "location": "",
                "url": normalize_url(e.get("link")),
                "source": "RSS",
                "posted_at": e.get("published") or e.get("updated"),
                "description": e.get("summary") or "",
            })
    except Exception:
        pass
    return out

# ---- Aggregators (optional keys) ----
def fetch_adzuna(cfg) -> List[Dict]:
    app_id = os.getenv("ADZUNA_APP_ID"); app_key = os.getenv("ADZUNA_APP_KEY")
    country = os.getenv("ADZUNA_COUNTRY","us")
    if not (app_id and app_key): return []
    what = "data analyst OR data scientist OR data engineer OR analytics engineer OR machine learning"
    url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/1?app_id={app_id}&app_key={app_key}&results_per_page=50&what={what}&where=United%20States&content-type=application/json"
    out=[]
    try:
        r = requests.get(url, headers=UA, timeout=25); r.raise_for_status()
        for j in r.json().get("results", []):
            jid = j.get("id") or j.get("adref")
            out.append({
                "id": f"adzuna:{jid}",
                "title": j.get("title"),
                "company": (j.get("company") or {}).get("display_name") or "",
                "location": (j.get("location") or {}).get("display_name") or "",
                "url": normalize_url(j.get("redirect_url")),
                "source": "Adzuna",
                "posted_at": j.get("created") or j.get("updated"),
                "description": j.get("description") or "",
            })
    except Exception:
        pass
    return out

def fetch_usajobs(cfg) -> List[Dict]:
    key = os.getenv("USAJOBS_API_KEY")
    email = os.getenv("USAJOBS_EMAIL")
    if not (key and email): return []
    headers = {"User-Agent": email, "Authorization-Key": key}
    kw = "data OR analytics OR machine learning"
    url = f"https://data.usajobs.gov/api/search?Keyword={kw}&Country=United%20States"
    out=[]
    try:
        r = requests.get(url, headers=headers, timeout=25); r.raise_for_status()
        data = r.json()
        for j in (data.get("SearchResult",{}).get("SearchResultItems") or []):
            item = j.get("MatchedObjectDescriptor",{})
            locs = ", ".join([loc.get("LocationName") for loc in item.get("PositionLocation",[])]) if item.get("PositionLocation") else ""
            out.append({
                "id": f"usajobs:{item.get('PositionID')}",
                "title": item.get("PositionTitle"),
                "company": item.get("OrganizationName","USAJOBS"),
                "location": locs,
                "url": normalize_url(item.get("PositionURI")),
                "source": "USAJOBS",
                "posted_at": item.get("PublicationStartDate"),
                "description": " ".join(item.get("UserArea",{}).get("Details",{}).get("JobSummary",[]) or []),
            })
    except Exception:
        pass
    return out

# ---- Master fetch ----
def fetch_all(cfg) -> List[Dict]:
    s = (cfg.get("sources") or {}) if isinstance(cfg.get("sources"), dict) else {}
    if not s or not any(s.get(k) for k in DEFAULT_SOURCES.keys()):
        s = DEFAULT_SOURCES

    out: List[Dict] = []
    for slug in (s.get("greenhouse_companies") or []): out.extend(fetch_greenhouse_company(slug))
    for slug in (s.get("lever_companies") or []):      out.extend(fetch_lever_company(slug))
    for sub  in (s.get("ashby_subdomains") or []):     out.extend(fetch_ashby(sub))
    for c    in (s.get("smartrecruiters_companies") or []): out.extend(fetch_smartrecruiters(c))
    for a    in (s.get("workable_accounts") or []):    out.extend(fetch_workable(a))
    for c    in (s.get("recruitee_companies") or []):  out.extend(fetch_recruitee(c))
    for sub  in (s.get("bamboohr_subdomains") or []):  out.extend(fetch_bamboohr(sub))
    for c    in (s.get("personio_companies") or []):   out.extend(fetch_personio(c))
    for url  in (s.get("rss_feeds") or []):            out.extend(fetch_rss(url))

    ag = (cfg.get("aggregators") or {})
    if (ag.get("adzuna") or {}).get("enabled"):  out.extend(fetch_adzuna(cfg))
    if (ag.get("usajobs") or {}).get("enabled"): out.extend(fetch_usajobs(cfg))

    return out
