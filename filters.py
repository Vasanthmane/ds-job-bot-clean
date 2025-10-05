import re, datetime
from datetime import timezone, timedelta

# ---- helpers ----
_NUMBER_WORDS = {"zero":0,"one":1,"two":2,"three":3,"four":4,"five":5,"six":6,"seven":7,"eight":8,"nine":9,
                 "ten":10,"eleven":11,"twelve":12,"thirteen":13,"fourteen":14,"fifteen":15,"sixteen":16,
                 "seventeen":17,"eighteen":18,"nineteen":19,"twenty":20}
def _num_from_word(w: str): return _NUMBER_WORDS.get((w or "").lower().strip())
def _normalize_text(s: str) -> str:
    if not s: return ""
    s = s.replace("\u2013","-").replace("\u2014","-").replace("–","-").replace("—","-")
    s = s.replace("\u00a0"," ")
    return " ".join(s.split())
def safe_lower(s: str) -> str: return (_normalize_text(s) or "").lower()

# ---- dates ----
def parse_when(when):
    if when in (None, ""): return None
    try:
        if isinstance(when,(int,float)):
            ts = float(when)
            if ts > 10_000_000_000: ts /= 1000.0
            return datetime.datetime.fromtimestamp(ts, tz=timezone.utc)
        s = str(when).strip()
        try:
            dt = datetime.datetime.fromisoformat(s.replace("Z","+00:00"))
            if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
            else: dt = dt.astimezone(timezone.utc)
            return dt
        except Exception:
            pass
        for fmt in ("%a, %d %b %Y %H:%M:%S %Z", "%a, %d %b %Y %H:%M:%S %z"):
            try:
                dt = datetime.datetime.strptime(s, fmt)
                if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
                else: dt = dt.astimezone(timezone.utc)
                return dt
            except Exception:
                continue
        return datetime.datetime.strptime(s[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return None

def is_within_days(when, days=31) -> bool:
    dt = parse_when(when)
    if not dt: return False
    return dt >= (datetime.datetime.now(timezone.utc) - timedelta(days=int(days)))

# ---- title filters ----
def title_includes_required(title: str, required_terms) -> bool:
    if not required_terms: return True
    t = safe_lower(title)
    return any(term.lower() in t for term in required_terms)

def title_level_is_ok(title: str, blocklist) -> bool:
    t = " " + safe_lower(title) + " "
    for bad in (blocklist or []):
        if (" " + bad.lower().strip() + " ") in t:
            return False
    if re.search(r"\b(senior|staff|principal|lead|architect|director|manager|sr\.?)\b", t):
        return False
    return True

# ---- clearance ----
def violates_clearance(job, blocklist) -> bool:
    text = safe_lower(f"{job.get('title','')} {job.get('description','')}")
    return any((term or "").lower() in text for term in (blocklist or []))

# ---- domain gating ----
_EXTRA_INCLUDE = [
    "data scientist","data analyst","data engineer","analytics engineer","bi analyst","bi developer",
    "business intelligence","quant","quantitative","research scientist","ml","machine learning",
    "deep learning","nlp","computer vision","etl","elt","data warehouse","data warehousing",
    "data platform","data governance","looker","tableau","power bi","snowflake","spark","dbt","airflow",
    "bigquery","sql","python","r"
]
def is_datasci(job, include_kw, exclude_kw) -> bool:
    text = safe_lower(f"{job.get('title','')} {job.get('description','')}")
    if any((x or "").lower() in text for x in (exclude_kw or [])): return False
    inc = set((include_kw or [])) | set(_EXTRA_INCLUDE)
    return any(x.lower() in text for x in inc)

# ---- US detector (Canada excluded) ----
_US_STATE_NAMES = ("alabama","alaska","arizona","arkansas","california","colorado","connecticut","delaware","florida",
"georgia","hawaii","idaho","illinois","indiana","iowa","kansas","kentucky","louisiana","maine","maryland","massachusetts",
"michigan","minnesota","mississippi","missouri","montana","nebraska","nevada","new hampshire","new jersey","new mexico",
"new york","north carolina","north dakota","ohio","oklahoma","oregon","pennsylvania","rhode island","south carolina",
"south dakota","tennessee","texas","utah","vermont","virginia","washington","west virginia","wisconsin","wyoming",
"district of columbia","washington, dc","washington dc","dc")
_US_ABBR = ("AL","AK","AZ","AR","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","DC")
_CA_PROV_ABBR = ("AB","BC","MB","NB","NL","NS","NT","NU","ON","PE","QC","SK","YT")
_CA_PROV_NAMES = ("alberta","british columbia","manitoba","new brunswick","newfoundland","nova scotia","northwest territories","nunavut","ontario","prince edward island","quebec","saskatchewan","yukon")

def is_us_job(job) -> bool:
    blob = " ".join([job.get("location") or "", job.get("title") or "", job.get("description") or ""])
    t = safe_lower(blob)
    if "canada" in t or "canadian" in t: return False
    for n in _CA_PROV_NAMES:
        if n in t: return False
    if re.search(r"(,|\s|-)\s*(AB|BC|MB|NB|NL|NS|NT|NU|ON|PE|QC|SK|YT)(?:\b|,)", t, re.I): return False
    if re.search(r"\b[A-Z]{2}\b\s*,\s*ca\b", t, re.I): return False
    if re.search(r",\s*ca\b", t) and ("ontario" in t or "quebec" in t or "british columbia" in t): return False
    if re.search(r"\b(remote\s*[-–]\s*canada|canada\s*\(remote\)|canada[- ]only)\b", t): return False
    if re.search(r"\b(united states|u\.s\.a\.|u\.s\.|usa|us[- ]only|us[- ]based)\b", t): return True
    if re.search(r"\b(remote\s*[-–]\s*us|us\s*remote)\b", t): return True
    for n in _US_STATE_NAMES:
        if n in t: return True
    if re.search(r"(,|\s|-)\s*(%s)(?:\b|,)" % "|".join(_US_ABBR), t, re.I): return True
    return False

# ---- sponsorship (ranking only) ----
_POS_SPONSOR = ["sponsor","sponsorship","h1b","h-1b","visa support","visa sponsorship","work visa"]
_NEG_SPONSOR = ["no sponsorship","cannot sponsor","sponsorship unavailable","not provide sponsorship"]
def sponsorship_score(job, cfg=None) -> int:
    text = safe_lower(f"{job.get('title','')} {job.get('description','')}")
    score = 0
    if any(p in text for p in _POS_SPONSOR): score += 1
    if any(n in text for n in _NEG_SPONSOR): score -= 1
    return score

# ---- experience (<4 yrs strict) ----
_RANGE   = re.compile(r"\b(\d+)\s*(?:to|-|–|—)\s*(\d+)\s*(?:\+?\s*)?(?:years?|yrs?)\b", re.I)
_PLUS    = re.compile(r"\b(\d+)\s*\+\s*(?:years?|yrs?)\b", re.I)
_ATLEAST = re.compile(r"\b(at\s+least|minimum(?:\s+of)?|min\.)\s*(\d+)\s*(?:years?|yrs?)\b", re.I)
_SIMPLE  = re.compile(r"\b(\d+)\s*(?:years?|yrs?)\s+(?:of\s+)?(?:experience|exp)\b", re.I)
_IN_X    = re.compile(r"\b(\d+)\s*(?:years?|yrs?)\s+(?:in|with|hands[- ]on)\b", re.I)
_WORDS   = re.compile(r"\b(zero|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty)\s+(?:years?|yrs?)\b", re.I)
_COMPACT = re.compile(r"\b(\d+)\s*[- ]?\s*(?:yr|yrs)\b", re.I)

def _extract_years_all(text: str):
    if not text: return []
    t = safe_lower(text)
    years = []
    for m in _RANGE.finditer(t):
        try: years.append(max(int(m.group(1)), int(m.group(2))))
        except: pass
    for m in _PLUS.finditer(t):
        try: years.append(int(m.group(1)))
        except: pass
    for m in _ATLEAST.finditer(t):
        try: years.append(int(m.group(2)))
        except: pass
    for m in _SIMPLE.finditer(t):
        try: years.append(int(m.group(1)))
        except: pass
    for m in _IN_X.finditer(t):
        try: years.append(int(m.group(1)))
        except: pass
    for m in _COMPACT.finditer(t):
        try: years.append(int(m.group(1)))
        except: pass
    for m in _WORDS.finditer(t):
        v = _num_from_word(m.group(1))
        if v is not None: years.append(int(v))
    if re.search(r"\b(mid[- ]senior|senior-level|staff|principal|lead)\b", t): years.append(4)
    return years

def meets_experience_max(job, max_years=3) -> bool:
    text = f"{job.get('title','')} {job.get('description','')}"
    ys = _extract_years_all(text)
    if not ys: return True
    return max(ys) < int(max_years)
