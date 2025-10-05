import re
from urllib.parse import urlsplit, urlunsplit

def safe_lower(s: str) -> str:
    return (s or "").replace("\u00a0"," ").strip().lower()

def normalize_url(url: str) -> str:
    if not url: return ""
    try:
        parts = urlsplit(url.strip())
        query = re.sub(r"(utm_[^=&]+|fbclid|gclid)=[^&]+&?", "", parts.query or "", flags=re.I)
        query = query.strip("&")
        return urlunsplit((parts.scheme, parts.netloc, parts.path, query, ""))
    except Exception:
        return url

def text_blob(*parts) -> str:
    return " ".join([(p or "").strip() for p in parts if p])
