from utils import safe_lower

_BUCKETS = [
    ("Data Engineering", ["data engineer","data platform","etl","elt","pipeline","spark","airflow","dbt","warehouse","bigquery","redshift","snowflake"]),
    ("Data Science / ML", ["data scientist","machine learning","ml engineer","mlops","deep learning","pytorch","tensorflow","nlp","computer vision","predictive"]),
    ("Analytics (BI/Product)", ["data analyst","analytics","bi","business intelligence","product analyst","looker","tableau","power bi"]),
    ("Analytics Engineering", ["analytics engineer"]),
    ("Research / Scientist", ["research scientist","applied scientist"]),
    ("Other Data", []),
]

def categorize(title: str) -> str:
    t = safe_lower(title)
    for name, keys in _BUCKETS:
        if any(k in t for k in keys):
            return name
    return "Other Data"
