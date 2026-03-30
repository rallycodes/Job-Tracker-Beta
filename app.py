#!/usr/bin/env python3
"""
JobTracker Pro — Complete Scraping Engine v2.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Architecture:
  • Dedicated scraper per platform (no guessing, no LLM required for basic scraping)
  • API-first for sites that expose JSON/RSS
  • requests + BeautifulSoup for HTML sites (fast, reliable)
  • Playwright only for heavily JS-gated sites (LinkedIn, Wellfound, Glassdoor)
  • Full pagination on every site (no missed pages)
  • Multi-strategy HTML extraction (JSON-LD → cards → links fallback)
  • Concurrent threading for speed
"""

from flask import Flask, render_template, request, jsonify
import json, os, requests, time, re, random, sqlite3, threading, asyncio
from dotenv import load_dotenv
load_dotenv()

from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin, urlparse, quote_plus
from apscheduler.schedulers.background import BackgroundScheduler
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    print("⚠ Playwright not installed — JS-heavy sites will be skipped.")

# ═══════════════════════════════════════════════════════════════
#  APP CONFIG
# ═══════════════════════════════════════════════════════════════
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY") or os.urandom(24).hex()
PORT     = int(os.getenv("PORT", 5000))
DB_FILE  = "jobs.db"
SITES_FILE = "sites.json"
MAX_PAGES  = int(os.getenv("MAX_PAGES", 5))   # max pages to paginate per site
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 6)) # concurrent threads

# ═══════════════════════════════════════════════════════════════
#  JOB TAXONOMY  (professional-grade, word-boundary safe)
# ═══════════════════════════════════════════════════════════════
#
#  Each role has two lists:
#   "phrases" — multi-word or unambiguous role identifiers (strong signal)
#   "tech"    — technology keywords that only count when accompanied by a
#               job-context word (developer / engineer / programmer / etc.)
#
#  This design avoids false positives like:
#    go → google,  pm → company,  java → javascript,
#    node → knowledge,  css → process,  html → github
#
JOB_TAXONOMY = {
    "Technology": {
        "Frontend Developer": {
            "phrases": [
                "frontend developer", "frontend engineer", "front-end developer",
                "front-end engineer", "front end developer", "front end engineer",
                "ui developer", "ui engineer", "web developer", "web engineer",
                "client-side developer", "client side developer",
                "react developer", "react engineer", "reactjs developer",
                "vue developer", "vue engineer", "vuejs developer",
                "angular developer", "angular engineer", "angularjs developer",
                "svelte developer", "next.js developer", "nextjs developer",
                "nuxt developer", "javascript developer", "typescript developer",
                "html developer", "css developer",
                # Azerbaijani / Russian aliases (appear on AZ job sites)
                "veb proqramçı", "veb-proqramçı", "interfeys proqramçısı",
                "veb developer", "frontend proqramçı",
                "фронтенд разработчик", "веб разработчик", "веб-разработчик",
            ],
            "tech": [
                "react", "vue", "angular", "svelte", "nextjs", "next.js", "nuxt",
                "javascript", "typescript", "tailwind", "webpack", "vite",
                "jquery", "bootstrap", "sass", "scss",
            ],
        },
        "Backend Developer": {
            "phrases": [
                "backend developer", "backend engineer", "back-end developer",
                "back-end engineer", "back end developer", "back end engineer",
                "server-side developer", "server side developer",
                "api developer", "api engineer",
                "node.js developer", "nodejs developer", "node developer",
                "python developer", "python engineer", "django developer",
                "flask developer", "fastapi developer",
                "ruby developer", "rails developer", "ruby on rails developer",
                "php developer", "laravel developer",
                "java developer", "java engineer", "spring developer",
                "golang developer", "go developer", "go engineer",
                "rust developer", "rust engineer",
                "elixir developer", "scala developer", "kotlin developer",
                "c# developer", ".net developer", "asp.net developer",
                "graphql developer",
            ],
            "tech": [
                "django", "fastapi", "laravel", "spring boot", "golang",
                "node.js", "nodejs",
            ],
        },
        "Fullstack Developer": {
            "phrases": [
                "fullstack developer", "fullstack engineer",
                "full-stack developer", "full-stack engineer",
                "full stack developer", "full stack engineer",
                "mean stack developer", "mern stack developer",
                "lamp stack developer", "full stack", "fullstack",
                # Azerbaijani / Russian aliases
                "fullstack proqramçı", "tam stack proqramçı",
                "фулстек разработчик", "full stack разработчик",
            ],
            "tech": [],
        },
        "Mobile Developer": {
            "phrases": [
                "mobile developer", "mobile engineer",
                "ios developer", "ios engineer",
                "android developer", "android engineer",
                "swift developer", "swiftui developer",
                "flutter developer", "react native developer",
                "xamarin developer", "kotlin developer",
            ],
            "tech": ["flutter", "swiftui", "react native"],
        },
        "Machine Learning Engineer": {
            "phrases": [
                "machine learning engineer", "ml engineer", "ai engineer",
                "deep learning engineer", "data scientist",
                "nlp engineer", "computer vision engineer",
                "pytorch engineer", "tensorflow engineer",
                "generative ai engineer", "llm engineer", "mlops engineer",
                "research scientist", "applied scientist",
            ],
            "tech": [
                "pytorch", "tensorflow", "keras", "scikit-learn", "huggingface",
                "langchain", "llm", "mlops", "generative ai",
            ],
        },
        "Data Engineer": {
            "phrases": [
                "data engineer", "etl developer", "data pipeline engineer",
                "spark engineer", "hadoop engineer", "databricks engineer",
                "snowflake engineer", "bigquery engineer", "dbt engineer",
                "data warehouse engineer", "analytics engineer",
            ],
            "tech": [
                "apache spark", "apache kafka", "databricks", "snowflake",
                "bigquery", "airflow", "dbt",
            ],
        },
        "Data Analyst": {
            "phrases": [
                "data analyst", "business analyst", "bi developer", "bi analyst",
                "power bi developer", "tableau developer", "looker developer",
                "sql analyst", "reporting analyst", "analytics analyst",
            ],
            "tech": ["power bi", "tableau", "looker", "metabase"],
        },
        "DevOps Engineer": {
            "phrases": [
                "devops engineer", "site reliability engineer", "sre",
                "platform engineer", "infrastructure engineer",
                "kubernetes engineer", "docker engineer", "devsecops engineer",
                "ci/cd engineer", "jenkins engineer", "terraform engineer",
                "linux administrator",
            ],
            "tech": [
                "kubernetes", "terraform", "ansible", "helm", "jenkins",
                "github actions", "gitlab ci",
            ],
        },
        "Cloud Engineer": {
            "phrases": [
                "cloud engineer", "cloud architect", "solutions architect",
                "aws engineer", "azure engineer", "gcp engineer",
                "cloud infrastructure engineer", "cloud developer",
                "serverless engineer", "cloud security engineer",
            ],
            "tech": [
                "amazon web services", "microsoft azure", "google cloud platform",
            ],
        },
        "Cybersecurity Engineer": {
            "phrases": [
                "security engineer", "cybersecurity engineer", "information security engineer",
                "penetration tester", "pentester", "ethical hacker",
                "soc analyst", "security analyst", "network security engineer",
                "appsec engineer", "devsecops engineer", "vulnerability analyst",
            ],
            "tech": [
                "penetration testing", "metasploit", "splunk", "wireshark",
            ],
        },
        "QA / Test Engineer": {
            "phrases": [
                "qa engineer", "quality assurance engineer", "test engineer",
                "automation test engineer", "selenium engineer",
                "cypress engineer", "playwright test engineer",
                "manual tester", "sdet", "software tester", "qa analyst",
            ],
            "tech": ["selenium", "cypress", "playwright", "appium", "testng"],
        },
        "Database Administrator": {
            "phrases": [
                "database administrator", "dba", "database engineer",
                "postgresql administrator", "mysql administrator",
                "mongodb administrator", "oracle dba", "sql server dba",
                "nosql engineer",
            ],
            "tech": ["postgresql", "mysql", "mongodb", "cassandra", "redis"],
        },
        "UI/UX Designer": {
            "phrases": [
                "ui designer", "ux designer", "ui/ux designer",
                "product designer", "interaction designer",
                "figma designer", "design systems engineer",
                "visual designer", "user experience designer",
                "user interface designer",
                # Azerbaijani / Russian aliases
                "dizayner", "ui/ux dizayner", "veb dizayner",
                "дизайнер", "ui дизайнер", "ux дизайнер", "веб-дизайнер",
            ],
            "tech": ["figma", "sketch", "adobe xd", "invision"],
        },
        "Product Manager": {
            "phrases": [
                "product manager", "product owner", "technical product manager",
                "senior product manager", "product lead", "head of product",
                "scrum master", "agile coach",
            ],
            "tech": [],
        },
        "Software Engineer (General)": {
            "phrases": [
                "software engineer", "software developer",
                "software development engineer",
                "programmer", "application developer", "application engineer",
                # Azerbaijani / Russian aliases
                "proqramçı", "proqram mühəndisi", "tətbiq proqramçısı",
                "программист", "разработчик программного обеспечения", "разработчик по",
            ],
            "tech": [],
        },
    }
}

# ─── Compiled regex patterns for fast matching ───────────────────────────────
# We compile one pattern per role with all phrases joined as alternates,
# using word boundaries (\b) to prevent substring false positives.

def _compile_role_pattern(phrases: list) -> re.Pattern:
    """Compile a word-boundary-safe regex OR-pattern for a list of phrases."""
    # Escape each phrase and anchor with word boundaries
    escaped = [r"\b" + re.escape(p) + r"\b" for p in sorted(phrases, key=len, reverse=True)]
    return re.compile("|".join(escaped), re.IGNORECASE)

# Build lookup: category → title → (phrase_pattern, tech_keywords_set)
_ROLE_PATTERNS: dict = {}
for _cat, _titles in JOB_TAXONOMY.items():
    _ROLE_PATTERNS[_cat] = {}
    for _title, _data in _titles.items():
        _phrases = _data.get("phrases", [])
        _tech    = _data.get("tech", [])
        _pat     = _compile_role_pattern(_phrases) if _phrases else None
        _ROLE_PATTERNS[_cat][_title] = {
            "pattern": _pat,
            "phrases": set(p.lower() for p in _phrases),
            "tech":    set(t.lower() for t in _tech),
        }

# Job-context words — a technology keyword only fires if one of these
# words is also in the text (prevents "java" matching a marketing post
# that mentions 'JavaScript' in passing).
_JOB_CONTEXT_RE = re.compile(
    r"\b(developer|engineer|programmer|dev\b|architect|consultant|"
    r"specialist|technician|analyst|scientist|designer|administrator|admin|dba)\b",
    re.IGNORECASE
)

# Legacy flat list (kept for fallback / RSS scrapers)
ALL_KEYWORDS = sorted(
    set(
        p.lower()
        for cat_roles in JOB_TAXONOMY.values()
        for role_data in cat_roles.values()
        for p in role_data.get("phrases", [])
    ),
    key=len, reverse=True
)


def get_kws_for(job_title: str, category: str) -> dict:
    """
    Returns a dict with:
      'pattern'  : compiled regex (OR of all phrases for matched roles)
      'tech'     : set of tech keywords
      'phrases'  : flat set of phrase strings
      'title'    : the job_title string
    
This is now the canonical matcher — used by matches_kws().
    """
    matched_phrases: set = set()
    matched_tech:    set = set()

    jt_lower = job_title.lower().strip() if job_title else ""

    for cat, roles in _ROLE_PATTERNS.items():
        # Category filter (case-insensitive)
        if category and cat.lower() != category.lower():
            continue

        for title, data in roles.items():
            tl = title.lower()
            # Match if:
            #   a) no job_title set (catch-all)
            #   b) job_title is a substring of taxonomy title
            #   c) taxonomy title is a substring of job_title
            #   d) any word from job_title appears in taxonomy title
            #   e) entire job_title is a phrase in this role
            if not jt_lower:
                matched_phrases.update(data["phrases"])
                matched_tech.update(data["tech"])
            else:
                hit = (
                    jt_lower in tl or
                    tl in jt_lower or
                    any(word in tl for word in re.split(r"[\s/\-]+", jt_lower) if len(word) > 2) or
                    jt_lower in data["phrases"]
                )
                if hit:
                    matched_phrases.update(data["phrases"])
                    matched_tech.update(data["tech"])

    # Always include the literal job_title words as phrase seeds
    if jt_lower:
        matched_phrases.add(jt_lower)

    if not matched_phrases and not matched_tech:
        # Absolute fallback — use all phrases across all roles
        for cat, roles in _ROLE_PATTERNS.items():
            for title, data in roles.items():
                matched_phrases.update(data["phrases"])
                matched_tech.update(data["tech"])

    pat = _compile_role_pattern(list(matched_phrases)) if matched_phrases else None
    return {
        "pattern":  pat,
        "phrases":  matched_phrases,
        "tech":     matched_tech,
        "title":    job_title or "",
    }


def matches_kws(text: str, kws) -> bool:
    """
    Backward-compatible wrapper. `kws` can be:
      - a dict (new format from get_kws_for)
      - a list (legacy — used by RSS scraper)
    Returns True if text matches any phrase/keyword.
    """
    if not text:
        return False
    t = text.lower()

    # Legacy list format
    if isinstance(kws, list):
        return any(kw in t for kw in kws)

    # New dict format
    pat = kws.get("pattern")
    if pat and pat.search(text):
        return True

    # Tech keywords only count if accompanied by a job-context word
    if kws.get("tech") and _JOB_CONTEXT_RE.search(text):
        for tk in kws["tech"]:
            if re.search(r"\b" + re.escape(tk) + r"\b", text, re.IGNORECASE):
                return True

    return False


def score_job_match(title: str, description: str, kws: dict) -> float:
    """
    Returns a confidence score 0.0–1.0.
    Used to rank/filter ambiguous results.
    0.0   = no match
    0.5+  = probable match (tech keyword + context)
    0.85+ = strong match (phrase hit in title)
    """
    if not title:
        return 0.0
    if isinstance(kws, list):
        # Legacy — just boolean
        return 1.0 if matches_kws(title, kws) else 0.0

    full_text = f"{title} {description or ''}"
    pat       = kws.get("pattern")

    # Strong: phrase match in title
    if pat and pat.search(title):
        return 1.0

    # Good: phrase in full text
    if pat and pat.search(full_text):
        return 0.85

    # Moderate: tech keyword + context word
    if kws.get("tech") and _JOB_CONTEXT_RE.search(full_text):
        for tk in kws["tech"]:
            if re.search(r"\b" + re.escape(tk) + r"\b", full_text, re.IGNORECASE):
                return 0.6

    return 0.0

# Minimum score threshold to accept a job as matching
MATCH_THRESHOLD = 0.5


# ═══════════════════════════════════════════════════════════════
#  DATABASE
# ═══════════════════════════════════════════════════════════════
def get_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS jobs (
        id       INTEGER PRIMARY KEY AUTOINCREMENT,
        job_url  TEXT UNIQUE,
        source   TEXT,
        title    TEXT,
        company  TEXT,
        salary   TEXT,
        date     TEXT,
        status   TEXT,
        added    TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS settings (
        id            INTEGER PRIMARY KEY,
        job_title     TEXT,
        job_field     TEXT,
        category      TEXT,
        az_active     INTEGER DEFAULT 0,
        global_active INTEGER DEFAULT 1
    )""")
    c.execute("SELECT id FROM settings LIMIT 1")
    if not c.fetchone():
        c.execute("INSERT INTO settings VALUES (1,'Frontend Developer','Technology','Technology',0,1)")
    for col, definition in [
        ("category",      "TEXT DEFAULT 'Technology'"),
        ("global_active", "INTEGER DEFAULT 1"),
    ]:
        try:
            c.execute(f"ALTER TABLE settings ADD COLUMN {col} {definition}")
        except Exception:
            pass
    conn.commit()
    conn.close()

def get_settings():
    conn = get_db()
    r = conn.execute("SELECT * FROM settings WHERE id=1").fetchone()
    conn.close()
    d = dict(r) if r else {}
    d.setdefault("global_active", 1)
    d.setdefault("az_active", 0)
    d.setdefault("job_title", "")
    d.setdefault("category", "Technology")
    return d

# ═══════════════════════════════════════════════════════════════
#  HTTP SESSION & UTILITIES
# ═══════════════════════════════════════════════════════════════
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:124.0) Gecko/20100101 Firefox/124.0",
]
_ua_idx = 0

def _next_ua():
    global _ua_idx
    a = USER_AGENTS[_ua_idx % len(USER_AGENTS)]
    _ua_idx += 1
    return a

def make_session() -> requests.Session:
    """Create a requests session with retry logic and random UA."""
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=1.5,
                  status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET", "POST"])
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

def html_headers(referer: str = "https://www.google.com/") -> dict:
    return {
        "User-Agent": _next_ua(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,az;q=0.8,ru;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Referer": referer,
        "Upgrade-Insecure-Requests": "1",
    }

def json_headers() -> dict:
    return {
        "User-Agent": _next_ua(),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
    }

def safe_get(session, url, *, is_json=False, timeout=25, **kwargs) -> requests.Response | None:
    try:
        hdrs = json_headers() if is_json else html_headers()
        r = session.get(url, headers=hdrs, timeout=timeout, allow_redirects=True, **kwargs)
        if 200 <= r.status_code < 300:
            return r
        print(f"  HTTP {r.status_code}: {url[:80]}")
    except Exception as e:
        print(f"  GET error [{url[:60]}]: {str(e)[:60]}")
    return None

def _domain(url: str) -> str:
    return urlparse(url).netloc.lower().replace("www.", "")

# ═══════════════════════════════════════════════════════════════
#  JOB NORMALISATION
# ═══════════════════════════════════════════════════════════════
def normalize_job(title: str, company: str, link: str, source: str,
                  salary: str = "N/A", date: str = "") -> dict:
    if not date:
        date = datetime.today().strftime("%Y-%m-%d")
    return {
        "title":   title.strip()[:300],
        "company": (company or "N/A").strip()[:200],
        "link":    link.strip(),
        "salary":  (salary or "N/A").strip()[:100],
        "date":    date[:10] if date else datetime.today().strftime("%Y-%m-%d"),
        "source":  source,
        "status":  "Not Applied",
        "added":   datetime.now().isoformat(),
    }

# ═══════════════════════════════════════════════════════════════
#  UNIVERSAL HTML JOB EXTRACTOR
#  Used by all HTML scrapers as fallback
# ═══════════════════════════════════════════════════════════════
def extract_jobs_html(soup: BeautifulSoup, base_url: str, source: str,
                      kws: list, search_filtered: bool = True) -> list:
    """
    Multi-strategy extractor:
      1. JSON-LD JobPosting schema
      2. Known card CSS selectors
      3. Link-based fallback
    search_filtered=True means the page was already filtered by a site search query,
    so we skip local kws check (avoids missing relevant jobs).
    """
    jobs = []
    seen_urls = set()

    def add(title, company, link, salary="N/A", date=""):
        if not title or not link or link in seen_urls:
            return
        if link.startswith("javascript:") or link.startswith("mailto:"):
            return
        if not link.startswith("http"):
            link = urljoin(base_url, link)
        if link in seen_urls:
            return
        seen_urls.add(link)
        jobs.append(normalize_job(title, company, link, source, salary, date))

    # ── Strategy 1: JSON-LD ──────────────────────────────────────────
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            raw = script.string or ""
            data = json.loads(raw)
            items = data if isinstance(data, list) else [data]
            for item in items:
                if isinstance(item, dict) and item.get("@type") == "JobPosting":
                    t = item.get("title", "")
                    u = item.get("url") or item.get("@id") or ""
                    org = item.get("hiringOrganization", {})
                    comp = org.get("name", "N/A") if isinstance(org, dict) else "N/A"
                    sal_raw = item.get("baseSalary")
                    sal = "N/A"
                    if isinstance(sal_raw, dict):
                        v = sal_raw.get("value", {})
                        if isinstance(v, dict):
                            sal = f"{v.get('minValue','')}–{v.get('maxValue','')} {v.get('unitText','')}".strip("– ")
                    if t and u and (search_filtered or matches_kws(t, kws)):
                        add(t, comp, u, sal)
        except Exception:
            pass

    if jobs:
        return jobs

    # ── Strategy 2: Card-based selectors ────────────────────────────
    CARD_SELECTORS = [
        # AZ sites — specific observed class names
        ".results-i","login","sign up",                         # boss.az
        ".vacancy-item", ".vacancy-list-item",  # work.az, rabota.az
        ".vacancies-list__item",                # jobsearch.az
        ".b-vacancy", ".b-vacancy-item",        # rabota.az alt layout
        ".job-item", ".job-card", ".job-listing",
        ".search-vacancy-item",                 # jobsearch.az alt
        ".vakansiya", ".vakansiya-item",        # Azerbaijani word
        ".is-elani", ".is-card",                # Azerbaijani "is elanı" (job ad)
        # Generic
        "[class*='job-card']", "[class*='jobcard']",
        "[class*='vacancy-card']", "[class*='vacancy-item']",
        "[class*='position-card']", "[class*='listing-item']",
        "[class*='job-post']", "[class*='job-row']",
        "article.job", "article[class*='job']",
        "li[class*='job']", "li[class*='vacanc']",
        "div[class*='result-item']", "div[class*='search-result']",
        ".job", ".vacancy", ".position",
    ]
    cards = []
    for sel in CARD_SELECTORS:
        try:
            cards = soup.select(sel)
            if len(cards) >= 2:
                break
        except Exception:
            pass

    if cards:
        # Validate the selector actually found job-like cards, not nav/footer elements.
        # A real job card must have an <a> tag with a non-empty href.
        cards = [c for c in cards if c.find("a", href=True)]
        # If the first card's only link is a nav/category URL (no ID or slug), discard batch
        if cards:
            sample_hrefs = [a["href"] for c in cards[:3] for a in c.find_all("a", href=True)]
            # Discard if none of the sample hrefs look like individual job pages
            looks_like_jobs = any(
                re.search(r"/\d{3,}|/vacancy/|/job/|/position/|/vacanc|/is-elan|/vakansiya", h)
                for h in sample_hrefs
            )
            if not looks_like_jobs and len(sample_hrefs) < 3:
                cards = []  # not real job cards — fall through to strategy 3

    if cards:
        for card in cards:
            # Title + link: try most specific first
            title_a = (
                card.select_one("h1 a, h2 a, h3 a, h4 a") or
                card.select_one("a.results-i-title") or
                card.select_one("a.job-title") or
                card.select_one("a.vacancy-title") or
                card.select_one("a.position-title") or
                card.select_one("a[class*='title']") or
                card.select_one("a[href*='/vacanc']") or
                card.select_one("a[href*='/job']") or
                card.select_one("a[href*='/position']") or
                card.select_one("a")
            )
            if not title_a:
                continue
            title = title_a.get_text(strip=True)
            href  = title_a.get("href", "")
            if not title or not href or len(title) < 3:
                continue
            if not search_filtered and not matches_kws(title, kws):
                continue

            comp_el = (
                card.select_one("a.results-i-company") or
                card.select_one("[class*='company']") or
                card.select_one("[class*='employer']") or
                card.select_one("[class*='org-name']")
            )
            sal_el = (
                card.select_one("[class*='salary']") or
                card.select_one("[class*='wage']") or
                card.select_one("[class*='pay']") or
                card.select_one("[class*='maas']")
            )
            date_el = (
                card.select_one("time[datetime]") or
                card.select_one("time") or
                card.select_one("[class*='date']") or
                card.select_one("[class*='posted']")
            )
            comp   = comp_el.get_text(strip=True) if comp_el else "N/A"
            sal    = sal_el.get_text(strip=True) if sal_el else "N/A"
            date   = (date_el.get("datetime") or date_el.get_text(strip=True)) if date_el else ""
            add(title, comp, href, sal, date)
        if jobs:
            return jobs

    # ── Strategy 3: Pure link extraction fallback ────────────────────
    JOB_URL_RE = re.compile(
        r"/(vacanc|vakansiya|is-elan|job[s]?|position[s]?|opening[s]?|career[s]?|apply|role[s]?)/",
        re.I
    )
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True)
        if not text or len(text) < 4 or len(text) > 200:
            continue
        if JOB_URL_RE.search(href) or re.search(r"/\d{4,}", href):
            if search_filtered or matches_kws(text, kws):
                add(text, "N/A", href)

    return jobs

def next_page_url(soup: BeautifulSoup, current_url: str, page: int) -> str | None:
    """Try to detect a 'next page' link from pagination."""
    # Explicit rel="next"
    nxt = soup.find("a", rel="next") or soup.find("link", rel="next")
    if nxt and nxt.get("href"):
        return urljoin(current_url, nxt["href"])
    # Common pagination class names
    for sel in [".next a", ".pagination-next a", "[class*='next-page'] a",
                ".pager .next a", "li.next a", "a[aria-label='Next']"]:
        el = soup.select_one(sel)
        if el and el.get("href"):
            return urljoin(current_url, el["href"])
    # Numeric page param: replace existing page=N or append fresh
    if re.search(r"[?&]page=\d+", current_url):
        return re.sub(r"(page=)\d+", f"page={page}", current_url)
    # Append page param (no duplicate risk now)
    sep = "&" if "?" in current_url else "?"
    return current_url + f"{sep}page={page}"

# ═══════════════════════════════════════════════════════════════
#  RSS SCRAPER (generic)
# ═══════════════════════════════════════════════════════════════
def scrape_rss(session: requests.Session, site: dict, kws: list) -> tuple[list, str]:
    r = safe_get(session, site["url"])
    if not r:
        return [], "failed to fetch"
    soup = None
    for parser in ["xml", "lxml-xml", "lxml", "html.parser"]:
        try:
            soup = BeautifulSoup(r.content, parser)
            if soup.find("item") or soup.find("entry"):
                break
        except Exception:
            pass
    if not soup:
        return [], "parse error"

    items = soup.find_all("item") or soup.find_all("entry")
    jobs = []
    for item in items:
        title_tag = item.find("title")
        title = title_tag.get_text(strip=True) if title_tag else ""
        # Remove CDATA
        title = re.sub(r"<!\[CDATA\[|\]\]>", "", title).strip()

        link_tag = item.find("link")
        if link_tag:
            link = link_tag.get("href") or link_tag.get_text(strip=True) or ""
        else:
            link = ""
        if not link:
            guid = item.find("guid") or item.find("id")
            link = guid.get_text(strip=True) if guid else ""

        if not title or not link:
            continue
        # Match on title OR full description text (RSS entries sometimes have vague titles)
        desc_tag = (item.find("description") or item.find("summary") or
                    item.find("content") or item.find("content:encoded"))
        desc_text = desc_tag.get_text(strip=True) if desc_tag else ""
        # Remove HTML tags from description
        desc_clean = re.sub(r"<[^>]+>", " ", desc_text)
        full_text = f"{title} {desc_clean}"
        if not matches_kws(full_text, kws):
            continue

        pub = item.find("pubDate") or item.find("published") or item.find("updated")
        date = ""
        if pub:
            raw = pub.get_text(strip=True)
            for fmt in ["%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S %Z",
                        "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ"]:
                try:
                    date = datetime.strptime(raw[:30], fmt).strftime("%Y-%m-%d")
                    break
                except Exception:
                    pass

        comp_tag = (item.find("company") or item.find("author") or
                    item.find("dc:creator") or item.find("creator"))
        company = comp_tag.get_text(strip=True) if comp_tag else "N/A"

        jobs.append(normalize_job(title, company, link, site["name"], "N/A", date))
    return jobs, f"rss ok: {len(jobs)} matched"

# ═══════════════════════════════════════════════════════════════
#  API SCRAPERS  (JSON endpoints — most reliable)
# ═══════════════════════════════════════════════════════════════

def scrape_remoteok(session: requests.Session, job_title: str, kws: list) -> tuple[list, str]:
    """remoteok.com has a public JSON API — no HTML scraping needed."""
    SOURCE = "Remote OK"
    r = safe_get(session, "https://remoteok.com/api", is_json=True,
                 timeout=30)
    if not r:
        return [], "api failed"
    try:
        data = r.json()
    except Exception:
        return [], "json parse error"
    jobs = []
    for item in data[1:]:  # first item is a legal notice dict
        if not isinstance(item, dict):
            continue
        title   = str(item.get("position", "")).strip()
        tags    = " ".join(item.get("tags") or [])
        text    = f"{title} {tags} {item.get('description','')}"
        if not matches_kws(text, kws):
            continue
        link  = item.get("url") or f"https://remoteok.com/remote-jobs/{item.get('id','')}"
        date  = str(item.get("date", ""))[:10]
        sal   = item.get("salary", "") or "N/A"
        comp  = item.get("company", "N/A")
        jobs.append(normalize_job(title, comp, link, SOURCE, str(sal), date))
    return jobs, f"api: {len(jobs)} matched"


def scrape_remotive(session: requests.Session, job_title: str, kws: list) -> tuple[list, str]:
    """remotive.com JSON API with optional search."""
    SOURCE = "Remotive"
    q = quote_plus(job_title) if job_title else ""
    url = f"https://remotive.com/api/remote-jobs?search={q}&limit=100"
    r = safe_get(session, url, is_json=True, timeout=30)
    if not r:
        return [], "api failed"
    try:
        data = r.json()
    except Exception:
        return [], "json parse error"
    jobs = []
    for item in data.get("jobs", []):
        title = item.get("title", "")
        if not matches_kws(f"{title} {item.get('tags','')} {item.get('category','')}", kws):
            continue
        link  = item.get("url", "")
        comp  = item.get("company_name", "N/A")
        sal   = item.get("salary", "N/A") or "N/A"
        date  = str(item.get("publication_date", ""))[:10]
        jobs.append(normalize_job(title, comp, link, SOURCE, sal, date))
    return jobs, f"api: {len(jobs)} matched"


def scrape_himalayas(session: requests.Session, job_title: str, kws: list) -> tuple[list, str]:
    """himalayas.app JSON API."""
    SOURCE = "Himalayas"
    q = quote_plus(job_title) if job_title else "developer"
    url = f"https://himalayas.app/jobs/api?q={q}&limit=100"
    r = safe_get(session, url, is_json=True, timeout=30)
    if not r:
        # Try without search
        r = safe_get(session, "https://himalayas.app/jobs/api?limit=100", is_json=True)
    if not r:
        return [], "api failed"
    try:
        data = r.json()
    except Exception:
        return [], "json parse error"
    jobs = []
    for item in data.get("jobs", []):
        title = item.get("title", "")
        if not matches_kws(f"{title} {' '.join(item.get('skills',[]))}", kws):
            continue
        link  = item.get("applicationLink") or item.get("url") or ""
        if not link:
            slug = item.get("slug", "")
            link = f"https://himalayas.app/jobs/{slug}" if slug else ""
        comp = item.get("company", {}).get("name", "N/A") if isinstance(item.get("company"), dict) else "N/A"
        sal  = item.get("salary", "N/A") or "N/A"
        date = str(item.get("createdAt", ""))[:10]
        jobs.append(normalize_job(title, comp, link, SOURCE, str(sal), date))
    return jobs, f"api: {len(jobs)} matched"


def scrape_arbeitsagentur(session, job_title, kws):
    """Placeholder — not applicable, skip."""
    return [], "skipped"


def scrape_jobicy(session, job_title, kws):
    """jobicy.com — public REST API, no scraping needed."""
    SOURCE = "Jobicy"
    BASE   = "https://jobicy.com"
    q      = (job_title or "developer").strip()
    # Tag-based search (most precise)
    url = f"{BASE}/api/v2/remote-jobs?tag={quote_plus(q)}&count=50&geo=worldwide"
    r   = safe_get(session, url, is_json=True, timeout=25)
    if not r:
        # Fallback: fetch latest without filter and match locally
        r = safe_get(session, f"{BASE}/api/v2/remote-jobs?count=50", is_json=True, timeout=25)
    if not r:
        return [], "api failed"
    try:
        data = r.json()
        jobs = []
        for item in data.get("jobs", []):
            title   = item.get("jobTitle", "").strip()
            tags    = " ".join(item.get("jobIndustry", []) if isinstance(item.get("jobIndustry"), list) else [])
            excerpt = item.get("jobExcerpt", "")
            text    = f"{title} {tags} {excerpt}"
            if not matches_kws(text, kws):
                continue
            link  = item.get("url", "")
            comp  = item.get("companyName", "N/A")
            sal   = str(item.get("annualSalaryMin", "") or item.get("salary", "") or "N/A")
            date  = str(item.get("pubDate", ""))[:10]
            if title and link:
                jobs.append(normalize_job(title, comp, link, SOURCE, sal, date))
        return jobs, f"api: {len(jobs)} matched"
    except Exception as e:
        return [], f"json error: {str(e)[:60]}"


# ═══════════════════════════════════════════════════════════════
#  AZERBAIJAN-SPECIFIC HTML SCRAPERS
#  Each uses site search + pagination + multi-selector fallback
# ═══════════════════════════════════════════════════════════════

def _az_paginate(session, start_url, source, kws, max_pages=MAX_PAGES,
                 delay_range=(1.5, 3.0)):
    """
    Generic AZ-site paginator.
    Fetches start_url, extracts jobs, follows pagination up to max_pages.
    Returns (jobs_list, info_str)
    """
    jobs = []
    url = start_url
    for page in range(1, max_pages + 1):
        time.sleep(random.uniform(*delay_range))
        r = safe_get(session, url, timeout=25)
        if not r:
            break
        soup = BeautifulSoup(r.text, "html.parser")
        base = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
        page_jobs = extract_jobs_html(soup, base, source, kws, search_filtered=True)
        if not page_jobs:
            break
        jobs.extend(page_jobs)
        print(f"    {source} p{page}: {len(page_jobs)} jobs")

        # Next page
        nxt_soup = soup
        nxt_url = next_page_url(nxt_soup, url, page + 1)
        if not nxt_url or nxt_url == url:
            break
        url = nxt_url
    return jobs, f"html: {len(jobs)} total"


def scrape_boss_az(session, job_title, kws):
    SOURCE = "Boss.az"
    BASE   = "https://boss.az"
    q = quote_plus(job_title or "developer")
    jobs = []
    seen = set()

    def _extract_cards(soup, source=SOURCE):
        extracted = []
        cards = soup.select(".results-i")
        if cards:
            for card in cards:
                title_a = (card.select_one("a.results-i-title") or
                           card.select_one("h3 a") or card.select_one("a"))
                if not title_a:
                    continue
                title = title_a.get_text(strip=True)
                href  = title_a.get("href", "")
                if not title or not href:
                    continue
                full  = urljoin(BASE, href)
                if full in seen:
                    continue
                seen.add(full)
                comp_a = card.select_one("a.results-i-company, [class*='company']")
                company = comp_a.get_text(strip=True) if comp_a else "N/A"
                sal_el  = card.select_one("[class*='salary'], [class*='price']")
                salary  = sal_el.get_text(strip=True) if sal_el else "N/A"
                date_el = card.select_one("time, [class*='date']")
                date    = (date_el.get("datetime") or date_el.get_text(strip=True)) if date_el else ""
                extracted.append(normalize_job(title, company, full, source, salary, date))
        else:
            for j in extract_jobs_html(soup, BASE, source, kws, search_filtered=True):
                if j["link"] not in seen:
                    seen.add(j["link"])
                    extracted.append(j)
        return extracted

    # Pass 1: keyword search
    for page in range(1, MAX_PAGES + 1):
        url = f"{BASE}/axtaris?search={q}&submit=1"
        if page > 1:
            url += f"&page={page}"
        time.sleep(random.uniform(1.5, 3.0))
        r = safe_get(session, url, timeout=25)
        if not r:
            break
        soup = BeautifulSoup(r.text, "html.parser")
        page_jobs = _extract_cards(soup)
        jobs.extend(page_jobs)
        print(f"    boss.az search p{page}: {len(page_jobs)}")
        if not page_jobs:
            break
        nxt = (soup.select_one(".pagination a[rel='next']") or
               soup.select_one(".pagination .next a") or
               soup.select_one("a.next"))
        if not nxt:
            break

    # Pass 2: IT category page — picks up jobs that keyword search misses
    # (e.g. jobs where the title is written in Azerbaijani or has unusual casing)
    # Try multiple slug variants — boss.az has changed this URL before
    it_url = None
    for slug in ["it-komputer-texnologiyalari", "it", "informasiya-texnologiyalari",
                 "proqramlasdirma", "komputer-texnologiyalari"]:
        probe = f"{BASE}/categories/{slug}"
        r_it = safe_get(session, probe, timeout=15)
        if r_it and "vacancy" in r_it.url.lower() or (r_it and r_it.status_code < 400):
            it_url = probe
            break
    if not it_url:
        it_url = f"{BASE}/categories/it-komputer-texnologiyalari"  # best guess fallback
    for page in range(1, MAX_PAGES + 1):
        url = it_url if page == 1 else f"{it_url}?page={page}"
        time.sleep(random.uniform(1.5, 3.0))
        r = safe_get(session, url, timeout=25)
        if not r:
            break
        soup = BeautifulSoup(r.text, "html.parser")
        page_jobs = _extract_cards(soup)
        # For category page, apply keyword filter since it's not pre-filtered
        page_jobs = [j for j in page_jobs if matches_kws(j["title"], kws)]
        jobs.extend(page_jobs)
        print(f"    boss.az IT cat p{page}: {len(page_jobs)}")
        if not page_jobs:
            break
        nxt = (soup.select_one(".pagination a[rel='next']") or
               soup.select_one(".pagination .next a") or
               soup.select_one("a.next"))
        if not nxt:
            break

    return jobs, f"html: {len(jobs)} total"


def scrape_jobsearch_az(session, job_title, kws):
    SOURCE = "Jobsearch.az"
    BASE   = "https://jobsearch.az"
    q = quote_plus(job_title or "developer")
    jobs = []
    seen = set()

    for page in range(1, MAX_PAGES + 1):
        url = f"{BASE}/vacancies?search={q}"
        if page > 1:
            url += f"&page={page}"
        time.sleep(random.uniform(1.5, 3.0))
        r = safe_get(session, url, timeout=25)
        if not r:
            break

        soup = BeautifulSoup(r.text, "html.parser")

        # jobsearch.az card selectors (observed in live HTML)
        cards = (soup.select(".vacancies-list__item") or
                 soup.select(".vacancy-item") or
                 soup.select("[class*='vacancy-item']"))
        page_jobs = []
        if cards:
            for card in cards:
                title_a = (card.select_one("a.vacancy-name") or
                           card.select_one("a.vacancy-title") or
                           card.select_one("h2 a, h3 a, h4 a") or
                           card.select_one("a[href*='/vacancy/']") or
                           card.select_one("a[href*='/vacancies/']") or
                           card.select_one("a"))
                if not title_a:
                    continue
                title = title_a.get_text(strip=True)
                href  = title_a.get("href", "")
                if not title or not href:
                    continue
                full = urljoin(BASE, href)
                comp_el = card.select_one("[class*='company'], [class*='employer']")
                company = comp_el.get_text(strip=True) if comp_el else "N/A"
                sal_el  = card.select_one("[class*='salary'], [class*='wage']")
                salary  = sal_el.get_text(strip=True) if sal_el else "N/A"
                date_el = card.select_one("time[datetime], time, [class*='date']")
                date    = (date_el.get("datetime") or date_el.get_text(strip=True)) if date_el else ""
                page_jobs.append(normalize_job(title, company, full, SOURCE, salary, date))
        else:
            page_jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=True)

        new = 0
        for j in page_jobs:
            if j["link"] not in seen:
                seen.add(j["link"])
                jobs.append(j)
                new += 1

        print(f"    jobsearch.az p{page}: {new}")
        if new == 0:
            break
        # Pagination: check for next link or numeric pager
        nxt = (soup.select_one("a[rel='next']") or
               soup.select_one(".pagination .next a") or
               soup.select_one("li.next a") or
               soup.select_one("a[aria-label='Next']"))
        if not nxt:
            break

    return jobs, f"html: {len(jobs)} total"


def scrape_work_az(session, job_title, kws):
    SOURCE = "Work.az"
    BASE   = "https://www.work.az"
    q = quote_plus(job_title or "developer")
    jobs = []
    seen = set()

    for page in range(1, MAX_PAGES + 1):
        url = f"{BASE}/vacancies?keyword={q}"
        if page > 1:
            url += f"&page={page}"
        time.sleep(random.uniform(1.5, 3.0))
        r = safe_get(session, url, timeout=25)
        if not r:
            break

        soup = BeautifulSoup(r.text, "html.parser")

        # work.az specific
        cards = soup.select(".vacancies-list .vacancy-item, .vacancy, [class*='vacancy-block']")
        page_jobs = []
        if cards:
            for card in cards:
                title_a = (card.select_one("a.vacancy-name") or
                           card.select_one("h3 a, h2 a") or
                           card.select_one("a[href*='/vacancies/']") or
                           card.select_one("a"))
                if not title_a:
                    continue
                title = title_a.get_text(strip=True)
                href  = title_a.get("href", "")
                if not title or not href:
                    continue
                full = urljoin(BASE, href)
                comp_el = card.select_one("[class*='company'], [class*='employer']")
                company = comp_el.get_text(strip=True) if comp_el else "N/A"
                sal_el  = card.select_one("[class*='salary'], [class*='wage']")
                salary  = sal_el.get_text(strip=True) if sal_el else "N/A"
                date_el = card.select_one("time, [class*='date']")
                date    = (date_el.get("datetime") or date_el.get_text(strip=True)) if date_el else ""
                page_jobs.append(normalize_job(title, company, full, SOURCE, salary, date))
        else:
            page_jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=True)

        new = 0
        for j in page_jobs:
            if j["link"] not in seen:
                seen.add(j["link"])
                jobs.append(j)
                new += 1

        print(f"    work.az p{page}: {new}")
        if new == 0:
            break
        # Check pagination — work.az uses multiple possible pagination patterns
        nxt = (soup.select_one("a[rel='next']") or
               soup.select_one(".pagination .next a") or
               soup.select_one("li.next a") or
               soup.select_one("a[aria-label='Next']"))
        if not nxt:
            break

    return jobs, f"html: {len(jobs)} total"


def scrape_rabota_az(session, job_title, kws):
    SOURCE = "Rabota.az"
    BASE   = "https://rabota.az"
    q = quote_plus(job_title or "developer")
    jobs = []
    seen = set()

    for page in range(1, MAX_PAGES + 1):
        url = f"{BASE}/vacancies?search={q}"
        if page > 1:
            url += f"&page={page}"
        time.sleep(random.uniform(1.5, 3.0))
        r = safe_get(session, url, timeout=25)
        if not r:
            break

        soup = BeautifulSoup(r.text, "html.parser")

        # rabota.az uses several possible layouts
        cards = (soup.select(".vacancy-item") or
                 soup.select(".job-item") or
                 soup.select("[class*='vacancy']") or [])

        page_jobs = []
        if cards:
            for card in cards:
                title_a = (card.select_one("a.vacancy-title, a.job-title") or
                           card.select_one("h3 a, h2 a") or card.select_one("a"))
                if not title_a:
                    continue
                title = title_a.get_text(strip=True)
                href  = title_a.get("href", "")
                if not title or not href:
                    continue
                full = urljoin(BASE, href)
                comp_el = card.select_one("[class*='company']")
                company = comp_el.get_text(strip=True) if comp_el else "N/A"
                sal_el  = card.select_one("[class*='salary']")
                salary  = sal_el.get_text(strip=True) if sal_el else "N/A"
                page_jobs.append(normalize_job(title, company, full, SOURCE, salary))
        else:
            page_jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=True)

        new = 0
        for j in page_jobs:
            if j["link"] not in seen:
                seen.add(j["link"])
                jobs.append(j)
                new += 1

        print(f"    rabota.az p{page}: {new}")
        if new == 0:
            break
        nxt = (soup.select_one("a[rel='next']") or
               soup.select_one(".pagination .next a") or
               soup.select_one("li.next a"))
        if not nxt:
            break

    return jobs, f"html: {len(jobs)} total"


def scrape_ejob_az(session, job_title, kws):
    SOURCE = "eJob.az"
    BASE   = "https://ejob.az"
    q = quote_plus(job_title or "developer")
    jobs = []
    seen = set()

    # ejob.az has been observed with multiple URL patterns across redesigns
    candidate_urls = [
        f"{BASE}/vakansiyalar?search={q}",   # Azerbaijani path
        f"{BASE}/jobs?search={q}",           # English path
        f"{BASE}/vacancies?search={q}",      # Alternative
        f"{BASE}/?s={q}",                    # WordPress-style search
    ]
    start_url = candidate_urls[0]
    # Quick probe: try each candidate until we get a 200 with some jobs
    for probe_url in candidate_urls:
        r_probe = safe_get(session, probe_url, timeout=20)
        if r_probe:
            soup_probe = BeautifulSoup(r_probe.text, "html.parser")
            probe_jobs = extract_jobs_html(soup_probe, BASE, SOURCE, kws, search_filtered=True)
            if probe_jobs:
                start_url = probe_url
                break

    for page in range(1, MAX_PAGES + 1):
        url = start_url
        if page > 1:
            sep = "&" if "?" in start_url else "?"
            url = start_url + f"{sep}page={page}"
        time.sleep(random.uniform(1.5, 3.0))
        r = safe_get(session, url, timeout=25)
        if not r:
            break
        soup = BeautifulSoup(r.text, "html.parser")
        page_jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=True)
        new = 0
        for j in page_jobs:
            if j["link"] not in seen:
                seen.add(j["link"])
                jobs.append(j)
                new += 1
        print(f"    ejob.az p{page}: {new}")
        if new == 0:
            break
        if not soup.select_one("a[rel='next'], .pagination .next, li.next a"):
            break

    return jobs, f"html: {len(jobs)} total"


def scrape_jobz_az(session, job_title, kws):
    SOURCE = "Jobz.az"
    BASE   = "https://jobz.az"
    q = quote_plus(job_title or "developer")
    jobs = []
    seen = set()

    # Probe correct search URL
    candidate_urls = [
        f"{BASE}/jobs?keyword={q}",
        f"{BASE}/vacancies?keyword={q}",
        f"{BASE}/jobs?search={q}",
        f"{BASE}/?s={q}",
    ]
    start_url = candidate_urls[0]
    for probe_url in candidate_urls:
        r_probe = safe_get(session, probe_url, timeout=20)
        if r_probe:
            soup_probe = BeautifulSoup(r_probe.text, "html.parser")
            if extract_jobs_html(soup_probe, BASE, SOURCE, kws, search_filtered=True):
                start_url = probe_url
                break

    for page in range(1, MAX_PAGES + 1):
        url = start_url
        if page > 1:
            sep = "&" if "?" in start_url else "?"
            url = start_url + f"{sep}page={page}"
        time.sleep(random.uniform(1.5, 3.0))
        r = safe_get(session, url, timeout=25)
        if not r:
            break
        soup = BeautifulSoup(r.text, "html.parser")
        page_jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=True)
        new = 0
        for j in page_jobs:
            if j["link"] not in seen:
                seen.add(j["link"])
                jobs.append(j)
                new += 1
        print(f"    jobz.az p{page}: {new}")
        if new == 0:
            break
        nxt = (soup.select_one("a[rel='next']") or
               soup.select_one(".pagination .next a") or
               soup.select_one("li.next a"))
        if not nxt:
            break

    return jobs, f"html: {len(jobs)} total"


def scrape_hcb_az(session, job_title, kws):
    SOURCE = "HCB.az"
    BASE   = "https://hcb.az"
    q = quote_plus(job_title or "developer")
    jobs = []
    seen = set()

    # Probe the correct search URL
    candidate_urls = [
        f"{BASE}/vacancies?search={q}",
        f"{BASE}/jobs?search={q}",
        f"{BASE}/search?q={q}",
        f"{BASE}/?s={q}",
    ]
    start_url = candidate_urls[0]
    for probe_url in candidate_urls:
        r_probe = safe_get(session, probe_url, timeout=20)
        if r_probe:
            soup_probe = BeautifulSoup(r_probe.text, "html.parser")
            if extract_jobs_html(soup_probe, BASE, SOURCE, kws, search_filtered=True):
                start_url = probe_url
                break

    for page in range(1, MAX_PAGES + 1):
        url = start_url
        if page > 1:
            sep = "&" if "?" in start_url else "?"
            url = start_url + f"{sep}page={page}"
        time.sleep(random.uniform(1.5, 3.0))
        r = safe_get(session, url, timeout=25)
        if not r:
            break
        soup = BeautifulSoup(r.text, "html.parser")
        page_jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=True)
        new = 0
        for j in page_jobs:
            if j["link"] not in seen:
                seen.add(j["link"])
                jobs.append(j)
                new += 1
        print(f"    hcb.az p{page}: {new}")
        if new == 0:
            break
        if not soup.select_one("a[rel='next'], .pagination .next"):
            break

    return jobs, f"html: {len(jobs)} total"


def scrape_navigator_az(session, job_title, kws):
    SOURCE = "Navigator.az"
    BASE   = "https://navigator.az"
    q = quote_plus(job_title or "developer")
    jobs = []
    seen = set()

    # navigator.az uses 'keyword' param, not 'q'
    candidate_urls = [
        f"{BASE}/en/jobsearch?keyword={q}",
        f"{BASE}/en/jobsearch?q={q}",
        f"{BASE}/en/jobs?keyword={q}",
    ]
    start_url = candidate_urls[0]
    for probe_url in candidate_urls:
        r_probe = safe_get(session, probe_url, timeout=20)
        if r_probe:
            soup_probe = BeautifulSoup(r_probe.text, "html.parser")
            if extract_jobs_html(soup_probe, BASE, SOURCE, kws, search_filtered=True):
                start_url = probe_url
                break

    for page in range(1, MAX_PAGES + 1):
        url = start_url
        if page > 1:
            sep = "&" if "?" in start_url else "?"
            url = start_url + f"{sep}page={page}"
        time.sleep(random.uniform(1.5, 3.0))
        r = safe_get(session, url, timeout=25)
        if not r:
            break
        soup = BeautifulSoup(r.text, "html.parser")
        page_jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=True)
        new = 0
        for j in page_jobs:
            if j["link"] not in seen:
                seen.add(j["link"])
                jobs.append(j)
                new += 1
        print(f"    navigator.az p{page}: {new}")
        if new == 0:
            break
        nxt = (soup.select_one("a[rel='next']") or
               soup.select_one(".pagination .next a") or
               soup.select_one("li.next a"))
        if not nxt:
            break

    return jobs, f"html: {len(jobs)} total"


def scrape_azjob_az(session, job_title, kws):
    SOURCE = "AZJob.az"
    BASE   = "https://azjob.az"
    q = quote_plus(job_title or "developer")
    jobs = []
    seen = set()

    # Probe correct search URL
    candidate_urls = [
        f"{BASE}/jobs?keyword={q}",
        f"{BASE}/jobs?search={q}",
        f"{BASE}/vacancies?search={q}",
        f"{BASE}/?s={q}",
    ]
    start_url = candidate_urls[0]
    for probe_url in candidate_urls:
        r_probe = safe_get(session, probe_url, timeout=20)
        if r_probe:
            soup_probe = BeautifulSoup(r_probe.text, "html.parser")
            if extract_jobs_html(soup_probe, BASE, SOURCE, kws, search_filtered=True):
                start_url = probe_url
                break

    for page in range(1, MAX_PAGES + 1):
        url = start_url
        if page > 1:
            sep = "&" if "?" in start_url else "?"
            url = start_url + f"{sep}page={page}"
        time.sleep(random.uniform(1.5, 3.0))
        r = safe_get(session, url, timeout=25)
        if not r:
            break
        soup = BeautifulSoup(r.text, "html.parser")
        page_jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=True)
        new = 0
        for j in page_jobs:
            if j["link"] not in seen:
                seen.add(j["link"])
                jobs.append(j)
                new += 1
        print(f"    azjob.az p{page}: {new}")
        if new == 0:
            break
        if not soup.select_one("a[rel='next'], .pagination .next"):
            break

    return jobs, f"html: {len(jobs)} total"


def scrape_nyasajob(session, job_title, kws):
    SOURCE = "NyasaJob.az"
    BASE   = "https://nyasajob.com"
    q = quote_plus(job_title or "developer")
    url = f"{BASE}/jobs?query={q}"
    return _az_paginate(session, url, SOURCE, kws)


# ═══════════════════════════════════════════════════════════════
#  GLOBAL HTML SCRAPERS (requests + BS4, no JS required)
# ═══════════════════════════════════════════════════════════════

def scrape_working_nomads(session, job_title, kws):
    SOURCE = "Working Nomads"
    BASE   = "https://www.workingnomads.com"
    q = quote_plus(job_title or "developer")
    url = f"{BASE}/jobs?keyword={q}&category=development"
    r = safe_get(session, url, timeout=25)
    if not r:
        return [], "failed"
    soup = BeautifulSoup(r.text, "html.parser")
    jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=True)
    return jobs, f"html: {len(jobs)}"


def scrape_nodesk(session, job_title, kws):
    SOURCE = "Node Desk"
    BASE   = "https://nodesk.co"
    url = f"{BASE}/remote-jobs/"
    r = safe_get(session, url, timeout=25)
    if not r:
        return [], "failed"
    soup = BeautifulSoup(r.text, "html.parser")
    jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=False)
    return jobs, f"html: {len(jobs)}"


def scrape_jsremotely(session, job_title, kws):
    SOURCE = "JS Remotely"
    BASE   = "https://jsremotely.com"
    r = safe_get(session, BASE, timeout=25)
    if not r:
        return [], "failed"
    soup = BeautifulSoup(r.text, "html.parser")
    jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=False)
    return jobs, f"html: {len(jobs)}"


def scrape_angular_jobs(session, job_title, kws):
    SOURCE = "Angular Jobs"
    BASE   = "https://angularjobs.com"
    r = safe_get(session, BASE, timeout=25)
    if not r:
        return [], "failed"
    soup = BeautifulSoup(r.text, "html.parser")
    jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=False)
    return jobs, f"html: {len(jobs)}"


def scrape_vuejobs(session, job_title, kws):
    SOURCE = "VueJobs"
    BASE   = "https://vuejobs.com"
    q = quote_plus(job_title or "vue")
    url = f"{BASE}/jobs?search={q}"
    r = safe_get(session, url, timeout=25)
    if not r:
        return [], "failed"
    soup = BeautifulSoup(r.text, "html.parser")
    jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=True)
    return jobs, f"html: {len(jobs)}"


def scrape_reactjsjob(session, job_title, kws):
    SOURCE = "ReactJS Job"
    BASE   = "https://reactjsjob.com"
    r = safe_get(session, BASE, timeout=25)
    if not r:
        return [], "failed"
    soup = BeautifulSoup(r.text, "html.parser")
    jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=False)
    return jobs, f"html: {len(jobs)}"


def scrape_justremote(session, job_title, kws):
    SOURCE = "JustRemote"
    BASE   = "https://justremote.co"
    q = quote_plus(job_title or "developer")
    url = f"{BASE}/remote-developer-jobs?keywords={q}"
    r = safe_get(session, url, timeout=25)
    if not r:
        return [], "failed"
    soup = BeautifulSoup(r.text, "html.parser")
    jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=True)
    return jobs, f"html: {len(jobs)}"


def scrape_dailyremote(session, job_title, kws):
    SOURCE = "Daily Remote"
    BASE   = "https://dailyremote.com"
    slug   = (job_title or "software-development").lower().replace(" ", "-")
    url = f"{BASE}/remote-{slug}-jobs"
    jobs = []
    seen = set()
    for page in range(1, MAX_PAGES + 1):
        pg_url = url if page == 1 else f"{url}?page={page}"
        time.sleep(random.uniform(1.0, 2.0))
        r = safe_get(session, pg_url, timeout=25)
        if not r:
            break
        soup = BeautifulSoup(r.text, "html.parser")
        page_jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=True)
        new = 0
        for j in page_jobs:
            if j["link"] not in seen:
                seen.add(j["link"])
                jobs.append(j)
                new += 1
        if new == 0:
            break
    return jobs, f"html: {len(jobs)}"


def scrape_startup_jobs(session, job_title, kws):
    SOURCE = "Startup Jobs"
    BASE   = "https://startup.jobs"
    q = quote_plus(job_title or "developer")
    url = f"{BASE}/?q={q}&remote=true"
    r = safe_get(session, url, timeout=25)
    if not r:
        return [], "failed"
    soup = BeautifulSoup(r.text, "html.parser")
    jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=True)
    return jobs, f"html: {len(jobs)}"


def scrape_dice(session, job_title, kws):
    SOURCE = "Dice"
    BASE   = "https://www.dice.com"
    q = quote_plus(job_title or "software developer")
    url = f"{BASE}/jobs?q={q}&l=Remote&radius=30&radiusUnit=mi&page=1&pageSize=20&filters.postedDate=ONE&language=en"
    r = safe_get(session, url, timeout=25)
    if not r:
        return [], "failed"
    soup = BeautifulSoup(r.text, "html.parser")
    jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=True)
    # Dice also has JSON in a script tag
    for script in soup.find_all("script", id=re.compile("__NEXT_DATA__|initialState")):
        try:
            data = json.loads(script.string or "")
            results = (data.get("props", {}).get("pageProps", {})
                          .get("searchResults", {}).get("hits", []))
            for item in results:
                title = item.get("title", "")
                company = item.get("employerProfile", {}).get("name", "N/A")
                link = item.get("applyDataStatus", {}).get("applyData", {}).get("redirectUri", "")
                if not link:
                    link = f"{BASE}/jobs/detail/{item.get('id','')}"
                if title and link and matches_kws(title, kws):
                    jobs.append(normalize_job(title, company, link, SOURCE))
        except Exception:
            pass
    return jobs, f"html: {len(jobs)}"


def scrape_authentic_jobs(session, job_title, kws):
    SOURCE = "Authentic Jobs"
    BASE   = "https://authenticjobs.com"
    q = quote_plus(job_title or "developer")
    url = f"{BASE}/?keywords={q}&remote=1"
    r = safe_get(session, url, timeout=25)
    if not r:
        return [], "failed"
    soup = BeautifulSoup(r.text, "html.parser")
    jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=True)
    return jobs, f"html: {len(jobs)}"


def scrape_powertofly(session, job_title, kws):
    SOURCE = "PowerToFly"
    BASE   = "https://powertofly.com"
    q = quote_plus(job_title or "developer")
    url = f"{BASE}/jobs/?search={q}&remote=true"
    r = safe_get(session, url, timeout=25)
    if not r:
        return [], "failed"
    soup = BeautifulSoup(r.text, "html.parser")
    jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=True)
    return jobs, f"html: {len(jobs)}"


def scrape_smashing_magazine(session, job_title, kws):
    SOURCE = "Smashing Magazine Jobs"
    BASE   = "https://www.smashingmagazine.com"
    url = f"{BASE}/jobs/"
    r = safe_get(session, url, timeout=25)
    if not r:
        return [], "failed"
    soup = BeautifulSoup(r.text, "html.parser")
    jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=False)
    return jobs, f"html: {len(jobs)}"


def scrape_devremotely(session, job_title, kws):
    SOURCE = "Dev Remotely"
    BASE   = "https://devremotely.com"
    r = safe_get(session, BASE, timeout=25)
    if not r:
        return [], "failed"
    soup = BeautifulSoup(r.text, "html.parser")
    jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=False)
    return jobs, f"html: {len(jobs)}"


def scrape_realworkfromanywhere(session, job_title, kws):
    SOURCE = "Real Work Anywhere"
    BASE   = "https://www.realworkfromanywhere.com"
    url = f"{BASE}/remote-frontend-jobs/rss.xml"
    # Try RSS first
    r = safe_get(session, url, timeout=25)
    if r:
        site_obj = {"name": SOURCE, "url": url}
        return scrape_rss(session, site_obj, kws)
    # Fallback HTML
    r2 = safe_get(session, BASE, timeout=25)
    if not r2:
        return [], "failed"
    soup = BeautifulSoup(r2.text, "html.parser")
    jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=False)
    return jobs, f"html: {len(jobs)}"


def scrape_jobspresso(session, job_title, kws):
    SOURCE = "Jobspresso"
    url = "https://jobspresso.co/feed/"
    r = safe_get(session, url, timeout=25)
    if not r:
        return [], "failed"
    return scrape_rss(session, {"name": SOURCE, "url": url}, kws)


def scrape_remoteco(session, job_title, kws):
    SOURCE = "Remote.co"
    url = "https://remote.co/rss/jobs/"
    r = safe_get(session, url, timeout=25)
    if not r:
        return [], "failed"
    return scrape_rss(session, {"name": SOURCE, "url": url}, kws)


def scrape_weworkremotely(session, job_title, kws):
    SOURCE = "We Work Remotely"
    url = "https://weworkremotely.com/categories/remote-programming-jobs.rss"
    r = safe_get(session, url, timeout=25)
    if not r:
        return [], "failed"
    return scrape_rss(session, {"name": SOURCE, "url": url}, kws)


def scrape_crossover(session, job_title, kws):
    SOURCE = "Crossover"
    BASE   = "https://www.crossover.com"
    q = quote_plus(job_title or "developer")
    url = f"{BASE}/jobs?search={q}"
    r = safe_get(session, url, timeout=25)
    if not r:
        return [], "failed"
    soup = BeautifulSoup(r.text, "html.parser")
    jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=True)
    return jobs, f"html: {len(jobs)}"


def scrape_turing(session, job_title, kws):
    SOURCE = "Turing"
    BASE   = "https://www.turing.com"
    q = quote_plus(job_title or "developer")
    url = f"{BASE}/jobs?q={q}"
    r = safe_get(session, url, timeout=25)
    if not r:
        return [], "failed"
    soup = BeautifulSoup(r.text, "html.parser")
    jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=True)
    return jobs, f"html: {len(jobs)}"


def scrape_braintrust(session, job_title, kws):
    SOURCE = "Braintrust"
    BASE   = "https://app.usebraintrust.com"
    q = quote_plus(job_title or "developer")
    # Try their API first
    api_url = f"https://api.usebraintrust.com/api/jobs/?search={q}&limit=50"
    r = safe_get(session, api_url, is_json=True, timeout=25)
    if r:
        try:
            data = r.json()
            jobs = []
            for item in data.get("results", []):
                title = item.get("title", "")
                comp  = item.get("client", {}).get("name", "N/A") if isinstance(item.get("client"), dict) else "N/A"
                link  = f"{BASE}/jobs/{item.get('id','')}/" if item.get("id") else ""
                sal   = item.get("payment", {}).get("hourly_rate_min", "N/A") if isinstance(item.get("payment"), dict) else "N/A"
                if title and link and matches_kws(title, kws):
                    jobs.append(normalize_job(title, comp, link, SOURCE, str(sal)))
            return jobs, f"api: {len(jobs)}"
        except Exception:
            pass
    # HTML fallback
    url = f"{BASE}/jobs/?search={q}"
    r2 = safe_get(session, url, timeout=25)
    if not r2:
        return [], "failed"
    soup = BeautifulSoup(r2.text, "html.parser")
    jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=True)
    return jobs, f"html: {len(jobs)}"


def scrape_skipthedrive(session, job_title, kws):
    SOURCE = "Skip The Drive"
    BASE   = "https://www.skipthedrive.com"
    q = quote_plus(job_title or "developer")
    url = f"{BASE}/remote-jobs/software-development/?s={q}"
    r = safe_get(session, url, timeout=25)
    if not r:
        return [], "failed"
    soup = BeautifulSoup(r.text, "html.parser")
    jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=True)
    return jobs, f"html: {len(jobs)}"


def scrape_jobbatical(session, job_title, kws):
    SOURCE = "Jobbatical"
    BASE   = "https://jobbatical.com"
    q = quote_plus(job_title or "developer")
    # Try their API
    api = f"https://jobbatical.com/api/explore?query={q}&limit=50"
    r = safe_get(session, api, is_json=True, timeout=25)
    if r:
        try:
            data = r.json()
            jobs = []
            items = data.get("jobs") or data.get("results") or []
            for item in items:
                title = item.get("title") or item.get("position", "")
                comp  = item.get("company", {}).get("name", "N/A") if isinstance(item.get("company"), dict) else "N/A"
                link  = item.get("url") or item.get("link") or f"{BASE}/explore"
                if title and matches_kws(title, kws):
                    jobs.append(normalize_job(title, comp, link, SOURCE))
            return jobs, f"api: {len(jobs)}"
        except Exception:
            pass
    # HTML fallback
    url = f"{BASE}/explore?query={q}"
    r2 = safe_get(session, url, timeout=25)
    if not r2:
        return [], "failed"
    soup = BeautifulSoup(r2.text, "html.parser")
    jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=True)
    return jobs, f"html: {len(jobs)}"


# ═══════════════════════════════════════════════════════════════
#  INDEED (AZ) — HTML scraper
# ═══════════════════════════════════════════════════════════════
def scrape_indeed_az(session, job_title, kws):
    SOURCE = "Indeed AZ"
    BASE   = "https://az.indeed.com"
    q = quote_plus(job_title or "developer")
    jobs = []
    seen = set()

    for page in range(0, MAX_PAGES * 10, 10):  # indeed paginates by 10
        url = f"{BASE}/jobs?q={q}&l=Azerbaijan&start={page}"
        time.sleep(random.uniform(2.0, 4.0))
        r = safe_get(session, url, timeout=25)
        if not r:
            break
        soup = BeautifulSoup(r.text, "html.parser")

        # Indeed specific card
        cards = soup.select(".job_seen_beacon, .jobsearch-SerpJobCard, [data-jk]")
        if not cards:
            cards = soup.select("[class*='job-card'], [class*='jobCard']")

        page_jobs = []
        for card in cards:
            title_el = (card.select_one("h2.jobTitle a") or
                        card.select_one("[class*='jobTitle'] a") or
                        card.select_one("a[data-jk]") or card.select_one("a"))
            if not title_el:
                continue
            title = title_el.get_text(strip=True)
            href  = title_el.get("href", "")
            if not title or not href:
                continue
            full = urljoin(BASE, href)
            comp_el = card.select_one(".companyName, [class*='company']")
            company = comp_el.get_text(strip=True) if comp_el else "N/A"
            sal_el  = card.select_one(".salary-snippet, [class*='salary']")
            salary  = sal_el.get_text(strip=True) if sal_el else "N/A"
            page_jobs.append(normalize_job(title, company, full, SOURCE, salary))

        if not page_jobs:
            page_jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=True)

        new = 0
        for j in page_jobs:
            if j["link"] not in seen:
                seen.add(j["link"])
                jobs.append(j)
                new += 1
        print(f"    indeed.az start={page}: {new}")
        if new == 0:
            break

    return jobs, f"html: {len(jobs)} total"


# ═══════════════════════════════════════════════════════════════
#  PLAYWRIGHT SCRAPERS  (JS-heavy sites)
# ═══════════════════════════════════════════════════════════════

_PW_SCRAPE_STATE = {"running": False}

async def pw_scrape_page(page, base_url: str, source: str, kws: list,
                         search_filtered: bool = True) -> list:
    """Extract jobs from a Playwright page object."""
    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")
    return extract_jobs_html(soup, base_url, source, kws, search_filtered)


async def scrape_arc_dev_pw(job_title: str, kws: list) -> tuple[list, str]:
    SOURCE = "Arc.dev"
    BASE   = "https://arc.dev"
    q = quote_plus(job_title or "developer")
    jobs = []
    seen = set()

    # Arc.dev has an undocumented JSON API used by their SPA — try it first
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(user_agent=_next_ua())
        page = await ctx.new_page()

        # Intercept API responses to capture the JSON data directly
        captured_jobs: list = []
        async def handle_response(response):
            if "api" in response.url and "jobs" in response.url:
                try:
                    data = await response.json()
                    items = (data.get("jobs") or data.get("data") or
                             data.get("results") or [])
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        title = item.get("title") or item.get("name") or ""
                        comp  = (item.get("company", {}).get("name", "N/A")
                                 if isinstance(item.get("company"), dict) else "N/A")
                        link  = item.get("url") or item.get("slug") or ""
                        if link and not link.startswith("http"):
                            link = f"{BASE}/remote-jobs/{link.lstrip('/')}"
                        if title and link and link not in seen:
                            seen.add(link)
                            captured_jobs.append(normalize_job(title, comp, link, SOURCE))
                except Exception:
                    pass
        page.on("response", handle_response)

        try:
            url = f"{BASE}/remote-jobs?keywords={q}"
            await page.goto(url, wait_until="networkidle", timeout=45000)
            await asyncio.sleep(3)

            # Use captured API data if available
            if captured_jobs:
                jobs.extend(captured_jobs)
            else:
                # Fall back to HTML extraction
                for pg in range(MAX_PAGES):
                    html = await page.content()
                    soup = BeautifulSoup(html, "html.parser")
                    page_jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=True)
                    new = 0
                    for j in page_jobs:
                        if j["link"] not in seen:
                            seen.add(j["link"])
                            jobs.append(j)
                            new += 1
                    print(f"    arc.dev p{pg+1}: {new}")
                    if new == 0:
                        break
                    nxt = await page.query_selector("a[rel='next'], button[aria-label='Next']")
                    if not nxt:
                        break
                    await nxt.click()
                    await asyncio.sleep(3)
        except Exception as e:
            print(f"arc.dev pw error: {e}")
        finally:
            await browser.close()
    return jobs, f"playwright: {len(jobs)}"


async def scrape_wellfound_pw(job_title: str, kws: list) -> tuple[list, str]:
    SOURCE = "Wellfound"
    BASE   = "https://wellfound.com"
    q = quote_plus(job_title or "developer")
    jobs = []
    seen = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(user_agent=_next_ua())
        page = await ctx.new_page()

        # Intercept GraphQL/API responses — Wellfound uses GraphQL
        captured: list = []
        async def handle_response(response):
            if "api" in response.url and response.status == 200:
                try:
                    ct = response.headers.get("content-type", "")
                    if "json" in ct:
                        data = await response.json()
                        # Wellfound GraphQL: data.data.talent_search.startups[].job_listings[]
                        startups = (data.get("data", {})
                                       .get("talent_search", {})
                                       .get("startups", []))
                        for startup in startups:
                            comp = startup.get("name", "N/A")
                            for listing in startup.get("job_listings", []):
                                title = listing.get("title", "")
                                slug  = listing.get("slug", "")
                                link  = f"{BASE}/jobs/{slug}" if slug else ""
                                sal   = listing.get("compensation", "") or "N/A"
                                if title and link and link not in seen:
                                    seen.add(link)
                                    captured.append(normalize_job(title, comp, link, SOURCE, sal))
                except Exception:
                    pass
        page.on("response", handle_response)

        try:
            url = f"{BASE}/jobs?query={q}&remote=true"
            await page.goto(url, wait_until="networkidle", timeout=45000)
            await asyncio.sleep(3)

            if captured:
                jobs.extend([j for j in captured if matches_kws(j["title"], kws)])
            else:
                for pg in range(MAX_PAGES):
                    html = await page.content()
                    soup = BeautifulSoup(html, "html.parser")
                    # Updated selectors for current Wellfound frontend
                    cards = (soup.select("[class*='JobListing']") or
                             soup.select("[class*='job-listing']") or
                             soup.select("div[data-test='StartupResult']") or
                             soup.select("[data-testid*='job']"))
                    page_jobs = []
                    if cards:
                        for card in cards:
                            title_a = (card.select_one("a[href*='/jobs/']") or
                                       card.select_one("a[href*='/l/']") or
                                       card.select_one("h2 a, h3 a"))
                            if not title_a:
                                continue
                            title = title_a.get_text(strip=True)
                            href  = title_a.get("href", "")
                            full  = urljoin(BASE, href)
                            if full in seen:
                                continue
                            comp_el = card.select_one("[class*='startupName'], [class*='company'], [class*='CompanyName']")
                            company = comp_el.get_text(strip=True) if comp_el else "N/A"
                            sal_el  = card.select_one("[class*='salary'], [class*='compensation'], [class*='Compensation']")
                            salary  = sal_el.get_text(strip=True) if sal_el else "N/A"
                            if matches_kws(title, kws):
                                seen.add(full)
                                page_jobs.append(normalize_job(title, company, full, SOURCE, salary))
                    else:
                        page_jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=True)
                        page_jobs = [j for j in page_jobs if j["link"] not in seen]
                        for j in page_jobs:
                            seen.add(j["link"])

                    jobs.extend(page_jobs)
                    print(f"    wellfound p{pg+1}: {len(page_jobs)}")
                    if not page_jobs:
                        break
                    nxt = await page.query_selector("a[aria-label='Next page'], button[aria-label='Next'], [data-test='next-page']")
                    if not nxt:
                        break
                    await nxt.click()
                    await asyncio.sleep(3)
        except Exception as e:
            print(f"wellfound pw error: {e}")
        finally:
            await browser.close()
    return jobs, f"playwright: {len(jobs)}"


async def scrape_glassdoor_pw(job_title: str, kws: list) -> tuple[list, str]:
    SOURCE = "Glassdoor AZ"
    BASE   = "https://www.glassdoor.com"
    q = quote_plus(job_title or "developer")
    jobs = []
    seen = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(
            user_agent=_next_ua(),
            locale="en-US",
            # Glassdoor is aggressive about bot detection — bypass with extra headers
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            }
        )
        page = await ctx.new_page()
        try:
            url = f"{BASE}/Job/azerbaijan-{quote_plus((job_title or 'developer').replace(' ','-'))}-jobs-SRCH_IL.0,10_IN17.htm"
            await page.goto(url, wait_until="domcontentloaded", timeout=45000)
            await asyncio.sleep(4)

            for pg in range(MAX_PAGES):
                html = await page.content()
                soup = BeautifulSoup(html, "html.parser")

                # Strategy 1: JSON-LD (most reliable — stable across Glassdoor redesigns)
                page_jobs = []
                for script in soup.find_all("script", type="application/ld+json"):
                    try:
                        data = json.loads(script.string or "")
                        items = data if isinstance(data, list) else [data]
                        for item in items:
                            if isinstance(item, dict) and item.get("@type") == "JobPosting":
                                title = item.get("title", "")
                                link  = item.get("url") or item.get("@id") or ""
                                org   = item.get("hiringOrganization", {})
                                comp  = org.get("name", "N/A") if isinstance(org, dict) else "N/A"
                                if title and link and link not in seen:
                                    seen.add(link)
                                    page_jobs.append(normalize_job(title, comp, link, SOURCE))
                    except Exception:
                        pass

                # Strategy 2: HTML card fallback
                if not page_jobs:
                    page_jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=True)
                    page_jobs = [j for j in page_jobs if j["link"] not in seen]
                    for j in page_jobs:
                        seen.add(j["link"])

                jobs.extend(page_jobs)
                print(f"    glassdoor p{pg+1}: {len(page_jobs)}")

                if not page_jobs:
                    break

                # Next page button
                nxt = await page.query_selector("button[data-test='pagination-next'], a[data-test='pagination-next']")
                if not nxt:
                    nxt = await page.query_selector("li[class*='nextButton'] button, [aria-label='Next page']")
                if not nxt:
                    break
                await nxt.click()
                await asyncio.sleep(3)

        except Exception as e:
            print(f"glassdoor pw error: {e}")
        finally:
            await browser.close()
    return jobs, f"playwright: {len(jobs)}"


async def scrape_linkedin_pw(job_title: str, kws: list, location: str = "Azerbaijan") -> tuple[list, str]:
    SOURCE = f"LinkedIn ({location})"
    BASE   = "https://www.linkedin.com"
    q      = quote_plus(job_title or "developer")
    loc    = quote_plus(location)
    jobs   = []
    seen   = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
        )
        ctx = await browser.new_context(
            user_agent=_next_ua(),
            locale="en-US",
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        # Mask navigator.webdriver to reduce bot detection
        await ctx.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
        page = await ctx.new_page()
        try:
            for start in range(0, MAX_PAGES * 25, 25):
                url = f"{BASE}/jobs/search/?keywords={q}&location={loc}&start={start}&f_TPR=r86400"
                await page.goto(url, wait_until="domcontentloaded", timeout=45000)
                await asyncio.sleep(random.uniform(3, 5))

                # Scroll to trigger lazy load
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(2)

                html = await page.content()
                soup = BeautifulSoup(html, "html.parser")

                # LinkedIn class names change but data attributes are stable
                cards = (
                    soup.select("[data-job-id]") or
                    soup.select(".job-search-card") or
                    soup.select("[class*='job-card-container']") or
                    soup.select("li[class*='result-card']")
                )

                page_jobs = []
                for card in cards:
                    title_a = (
                        card.select_one("h3 a") or
                        card.select_one("a.job-card-list__title") or
                        card.select_one("a[href*='/jobs/view/']") or
                        card.select_one("a[href*='/jobs/']")
                    )
                    if not title_a:
                        continue
                    title = title_a.get_text(strip=True)
                    href  = title_a.get("href", "")
                    # Strip tracking params from LinkedIn URLs
                    href  = href.split("?")[0] if "?" in href else href
                    full  = href if href.startswith("http") else urljoin(BASE, href)
                    if full in seen:
                        continue
                    comp_el = card.select_one(
                        ".job-card-container__company-name, "
                        "[class*='subtitle'], [class*='company-name']"
                    )
                    company = comp_el.get_text(strip=True) if comp_el else "N/A"
                    if matches_kws(title, kws):
                        seen.add(full)
                        page_jobs.append(normalize_job(title, company, full, SOURCE))

                # Fallback if no cards matched selectors
                if not page_jobs:
                    page_jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=True)
                    page_jobs = [j for j in page_jobs if j["link"] not in seen]
                    for j in page_jobs:
                        seen.add(j["link"])

                jobs.extend(page_jobs)
                print(f"    linkedin({location}) start={start}: {len(page_jobs)}")
                if not page_jobs:
                    break

        except Exception as e:
            print(f"linkedin pw error: {e}")
        finally:
            await browser.close()
    return jobs, f"playwright: {len(jobs)}"


# Generic Playwright fallback for any uncatalogued site
async def scrape_generic_pw(site: dict, job_title: str, kws: list) -> tuple[list, str]:
    SOURCE = site["name"]
    BASE   = f"{urlparse(site['url']).scheme}://{urlparse(site['url']).netloc}"
    q = quote_plus(job_title or "developer")
    url = site["url"]
    jobs = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(user_agent=_next_ua())
        page = await ctx.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=40000)
            await asyncio.sleep(2)
            # Try to find a search input
            for sel in ["input[type='search']", "input[name='q']",
                        "input[placeholder*='search' i]", "input[placeholder*='job' i]"]:
                try:
                    el = page.locator(sel).first
                    if await el.is_visible(timeout=2000):
                        await el.fill(job_title or "")
                        await el.press("Enter")
                        await asyncio.sleep(3)
                        break
                except Exception:
                    pass
            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")
            jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=bool(job_title))
        except Exception as e:
            print(f"generic_pw [{SOURCE}] error: {e}")
        finally:
            await browser.close()
    return jobs, f"playwright: {len(jobs)}"


# ═══════════════════════════════════════════════════════════════
#  SCRAPER REGISTRY
#  Maps domain pattern → scraper function
# ═══════════════════════════════════════════════════════════════

# sync scrapers: fn(session, job_title, kws) → (jobs, info)
SYNC_SCRAPERS: dict[str, callable] = {
    "remoteok.com":             scrape_remoteok,
    "remotive.com":             scrape_remotive,
    "himalayas.app":            scrape_himalayas,
    "weworkremotely.com":       lambda s,t,k: scrape_weworkremotely(s,t,k),
    "jobspresso.co":            lambda s,t,k: scrape_jobspresso(s,t,k),
    "remote.co":                lambda s,t,k: scrape_remoteco(s,t,k),
    "realworkfromanywhere.com": scrape_realworkfromanywhere,
    "nodesk.co":                scrape_nodesk,
    "workingnomads.com":        scrape_working_nomads,
    "jsremotely.com":           scrape_jsremotely,
    "angularjobs.com":          scrape_angular_jobs,
    "vuejobs.com":              scrape_vuejobs,
    "reactjsjob.com":           scrape_reactjsjob,
    "justremote.co":            scrape_justremote,
    "dailyremote.com":          scrape_dailyremote,
    "startup.jobs":             scrape_startup_jobs,
    "dice.com":                 scrape_dice,
    "powertofly.com":           scrape_powertofly,
    "authenticjobs.com":        scrape_authentic_jobs,
    "smashingmagazine.com":     scrape_smashing_magazine,
    "devremotely.com":          scrape_devremotely,
    "crossover.com":            scrape_crossover,
    "turing.com":               scrape_turing,
    "usebraintrust.com":        scrape_braintrust,
    "skipthedrive.com":         scrape_skipthedrive,
    "jobbatical.com":           scrape_jobbatical,
    "jobicy.com":               scrape_jobicy,
    # AZ sites
    "boss.az":                  scrape_boss_az,
    "jobsearch.az":             scrape_jobsearch_az,
    "work.az":                  scrape_work_az,
    "rabota.az":                scrape_rabota_az,
    "ejob.az":                  scrape_ejob_az,
    "jobz.az":                  scrape_jobz_az,
    "hcb.az":                   scrape_hcb_az,
    "navigator.az":             scrape_navigator_az,
    "azjob.az":                 scrape_azjob_az,
    "nyasajob.com":             scrape_nyasajob,
    "me.indeed.com":            scrape_indeed_az,
    "az.indeed.com":            scrape_indeed_az,
}

# async/playwright scrapers: fn(job_title, kws) → (jobs, info)
ASYNC_SCRAPERS: dict[str, callable] = {
    "arc.dev":        scrape_arc_dev_pw,
    "wellfound.com":  scrape_wellfound_pw,
    "glassdoor.com":  scrape_glassdoor_pw,
    # LinkedIn was defined but never registered — fixed here
    "linkedin.com":   lambda t, k: scrape_linkedin_pw(t, k, "Azerbaijan"),
}

def get_scraper(site: dict):
    """Return (kind, fn) — kind is 'sync', 'async', or 'generic_pw'."""
    dom = _domain(site["url"])
    # Strip subdomains: work.az, me.indeed.com → check both
    for pattern, fn in SYNC_SCRAPERS.items():
        if dom == pattern or dom.endswith("." + pattern):
            return "sync", fn
    for pattern, fn in ASYNC_SCRAPERS.items():
        if dom == pattern or dom.endswith("." + pattern):
            return "async", fn
    # RSS sites we haven't catalogued
    if site.get("type") == "rss":
        return "sync", lambda s, t, k: scrape_rss(s, site, k)
    return "generic_pw", None


# ═══════════════════════════════════════════════════════════════
#  SITE LIST MANAGEMENT
# ═══════════════════════════════════════════════════════════════
DEFAULT_SITES = [
    {"name": "We Work Remotely",      "url": "https://weworkremotely.com/categories/remote-programming-jobs.rss", "active": True, "type": "rss"},
    {"name": "Remote OK",             "url": "https://remoteok.com/rss",                "active": True, "type": "api"},
    {"name": "Remotive",              "url": "https://remotive.com/remote-jobs/feed/software-dev", "active": True, "type": "api"},
    {"name": "Jobspresso",            "url": "https://jobspresso.co/feed/",             "active": True, "type": "rss"},
    {"name": "Remote.co",             "url": "https://remote.co/rss/jobs/",             "active": True, "type": "rss"},
    {"name": "Real Work Anywhere",    "url": "https://www.realworkfromanywhere.com/remote-frontend-jobs/rss.xml", "active": True, "type": "rss"},
    {"name": "Himalayas",             "url": "https://himalayas.app/jobs/rss",          "active": True, "type": "api"},
    {"name": "Node Desk",             "url": "https://nodesk.co/remote-jobs/",          "active": True, "type": "crawler"},
    {"name": "Working Nomads",        "url": "https://www.workingnomads.com/jobs?category=development", "active": True, "type": "crawler"},
    {"name": "JS Remotely",           "url": "https://jsremotely.com/",                 "active": True, "type": "crawler"},
    {"name": "Angular Jobs",          "url": "https://angularjobs.com/",                "active": True, "type": "crawler"},
    {"name": "VueJobs",               "url": "https://vuejobs.com/",                   "active": True, "type": "crawler"},
    {"name": "ReactJS Jobs",          "url": "https://reactjsjob.com/",                 "active": True, "type": "crawler"},
    {"name": "JustRemote Dev",        "url": "https://justremote.co/remote-developer-jobs", "active": True, "type": "crawler"},
    {"name": "Daily Remote",          "url": "https://dailyremote.com/remote-software-development-jobs", "active": True, "type": "crawler"},
    {"name": "Arc.dev",               "url": "https://arc.dev/remote-jobs",             "active": True, "type": "playwright"},
    {"name": "Wellfound",             "url": "https://wellfound.com/role/r/software-engineer", "active": True, "type": "playwright"},
    {"name": "Jobbatical",            "url": "https://jobbatical.com/explore",          "active": True, "type": "crawler"},
    {"name": "Skip The Drive",        "url": "https://www.skipthedrive.com/remote-jobs/software-development/", "active": True, "type": "crawler"},
    {"name": "Crossover",             "url": "https://www.crossover.com/jobs",          "active": True, "type": "crawler"},
    {"name": "Turing",                "url": "https://www.turing.com/jobs",             "active": True, "type": "crawler"},
    {"name": "Braintrust",            "url": "https://app.usebraintrust.com/jobs/",     "active": True, "type": "crawler"},
    {"name": "Startup Jobs",          "url": "https://startup.jobs/?q=remote",          "active": True, "type": "crawler"},
    {"name": "Dice Remote",           "url": "https://www.dice.com/jobs?q=Software+Developer&l=Remote", "active": True, "type": "crawler"},
    {"name": "PowerToFly",            "url": "https://powertofly.com/jobs/",            "active": True, "type": "crawler"},
    {"name": "Authentic Jobs",        "url": "https://authenticjobs.com/",              "active": True, "type": "crawler"},
    {"name": "Smashing Magazine Jobs","url": "https://www.smashingmagazine.com/jobs/",  "active": True, "type": "crawler"},
    {"name": "Dev Remotely",          "url": "https://devremotely.com/",                "active": True, "type": "crawler"},
]

AZ_SITES = [
    {"name": "Boss.az",       "url": "https://boss.az",                                               "active": True, "type": "crawler", "az_pack": True, "expert_selectors": { "container": ".results-i", "title": ".results-i-title", "company": ".results-i-company" }},
    {"name": "Jobsearch.az",  "url": "https://jobsearch.az/vacancies",                               "active": True, "type": "crawler", "az_pack": True, "expert_selectors": { "container": "a.vacancies-item", "title": ".vacancies-item__title" }},
    {"name": "Work.az",       "url": "https://www.work.az",                                           "active": True, "type": "crawler", "az_pack": True},
    {"name": "Rabota.az",     "url": "https://rabota.az",                                             "active": True, "type": "crawler", "az_pack": True},
    {"name": "eJob.az",       "url": "https://ejob.az",                                               "active": True, "type": "crawler", "az_pack": True},
    {"name": "Jobz.az",       "url": "https://jobz.az",                                               "active": True, "type": "crawler", "az_pack": True},
    {"name": "Navigator.az",  "url": "https://navigator.az/en/jobsearch",                             "active": True, "type": "crawler", "az_pack": True},
    {"name": "HCB.az",        "url": "https://hcb.az",                                               "active": True, "type": "crawler", "az_pack": True},
    {"name": "AZJob.az",      "url": "https://azjob.az",                                             "active": True, "type": "crawler", "az_pack": True},
    {"name": "NyasaJob.az",   "url": "https://nyasajob.com",                                          "active": True, "type": "crawler", "az_pack": True},
    {"name": "LinkedIn AZ",   "url": "https://www.linkedin.com/jobs/search/?location=Azerbaijan",     "active": True, "type": "playwright", "az_pack": True, "expert_selectors": { "container": ".jobs-search__results-list li" }},
    {"name": "Glassdoor AZ",  "url": "https://www.glassdoor.com/Job/azerbaijan-jobs-SRCH_IL.0,10_IN17.htm", "active": True, "type": "playwright", "az_pack": True},
    {"name": "Indeed AZ",     "url": "https://az.indeed.com/jobs?l=Azerbaijan",                       "active": True, "type": "crawler", "az_pack": True},
]

def load_sites():
    try:
        with open(SITES_FILE) as f:
            data = json.load(f)
        if isinstance(data, list) and data:
            return data
    except Exception:
        pass
    # First run
    s = get_settings()
    initial = list(DEFAULT_SITES) if s.get("global_active", 1) else []
    if s.get("az_active"):
        initial.extend(AZ_SITES)
    save_sites(initial)
    return initial

def save_sites(data):
    with open(SITES_FILE, "w") as f:
        json.dump(data, f, indent=2)

def rebuild_sites(global_active: bool, az_active: bool, current_sites: list) -> list:
    global_domains = {_domain(x["url"]) for x in DEFAULT_SITES}
    az_domains     = {_domain(x["url"]) for x in AZ_SITES}
    custom = [s for s in current_sites
              if _domain(s["url"]) not in global_domains
              and _domain(s["url"]) not in az_domains]
    final = []
    if global_active:
        final.extend(DEFAULT_SITES)
    final.extend(custom)
    if az_active:
        final.extend(AZ_SITES)
    return final

def site_name_from_url(url):
    try:
        host = urlparse("https://" + url if not url.startswith("http") else url).netloc.lower().replace("www.", "")
        name = host.split(".")[0].replace("-", " ").replace("_", " ")
        return " ".join(w.capitalize() for w in name.split())
    except Exception:
        return "New Site"

# ═══════════════════════════════════════════════════════════════
#  SCRAPE STATE  (shared across threads)
# ═══════════════════════════════════════════════════════════════
_scrape_state = {
    "running":      False,
    "cancel":       False,
    "log":          [],
    "new_total":    0,
    "current_site": "",
    "progress":     0,
    "total":        0,
}
_state_lock = threading.Lock()

def _state_update(**kwargs):
    with _state_lock:
        _scrape_state.update(kwargs)

# ═══════════════════════════════════════════════════════════════
#  DATABASE WRITER  (thread-safe)
# ═══════════════════════════════════════════════════════════════
_db_lock = threading.Lock()

def save_jobs_to_db(jobs: list, kws: dict = None) -> int:
    """Insert jobs, skip duplicates. Returns count of newly added jobs.
    
    NOTE: kws parameter is kept for API compatibility but the score filter
    has been intentionally removed. Reasoning:
      • AZ scrapers scrape server-side search results (already pre-filtered)
      • Azerbaijani-language job titles ("Proqramçı", "Veb-proqramçı") score 0.0
        against English phrase patterns, so ALL AZ-language jobs were filtered out
      • Each scraper already does its own keyword matching via matches_kws()
    Filtering belongs in the scrapers, not at the write stage.
    """
    added = 0
    with _db_lock:
        conn = get_db()
        c = conn.cursor()
        for j in jobs:
            url = j.get("link", "").strip()
            if not url:
                continue
            title   = j.get("title", "").strip()
            company = j.get("company", "N/A").strip()
            try:
                c.execute(
                    """INSERT INTO jobs (job_url,source,title,company,salary,date,status,added)
                       VALUES (?,?,?,?,?,?,?,?)""",
                    (url, j.get("source",""), title, company,
                     j.get("salary","N/A"), j.get("date",""),
                     j.get("status","Not Applied"), j.get("added", datetime.now().isoformat()))
                )
                added += 1
            except sqlite3.IntegrityError:
                pass
        conn.commit()
        conn.close()
    return added

# ═══════════════════════════════════════════════════════════════
#  ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════

def _run_site(site: dict, job_title: str, kws: list) -> tuple[list, str]:
    """Run a single site's scraper and return (jobs, info)."""
    kind, fn = get_scraper(site)
    session = make_session()

    # Expert / AZ-Pack sites ALWAYS use the Expert Playwright engine
    if site.get("az_pack") and PLAYWRIGHT_AVAILABLE:
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(scrape_expert_pw(site, job_title, kws))
            loop.close()
            return result
        except Exception as e:
            return [], f"expert error: {str(e)[:80]}"

    if kind == "sync":
        try:
            return fn(session, job_title, kws)
        except Exception as e:
            return [], f"error: {str(e)[:80]}"

    elif kind == "async":
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(fn(job_title, kws))
            loop.close()
            return result
        except Exception as e:
            return [], f"async error: {str(e)[:80]}"

    else:
        # Generic Playwright
        if not PLAYWRIGHT_AVAILABLE:
            return [], "playwright not installed"
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(scrape_generic_pw(site, job_title, kws))
            loop.close()
            return result
        except Exception as e:
            return [], f"pw error: {str(e)[:80]}"


async def scrape_expert_pw(site: dict, job_title: str, kws: list) -> tuple[list, str]:
    """
    Expert Deep-Crawling Engine.
    - Sequential Tasking
    - Session Isolation
    - pagination / Infinite Scroll exhaustion
    - Specialist selector injection
    """
    SOURCE = site["name"]
    BASE   = f"{urlparse(site['url']).scheme}://{urlparse(site['url']).netloc}"
    url    = site["url"]
    jobs   = []
    seen   = set()
    
    expert = site.get("expert_selectors", {})
    cont_sel  = expert.get("container")
    title_sel = expert.get("title")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
        )
        # ISOLATION: Fresh context per call
        vports = [{"width": 1920, "height": 1080}, {"width": 1440, "height": 900}, {"width": 1366, "height": 768}]
        ctx = await browser.new_context(
            user_agent=_next_ua(),
            viewport=random.choice(vports),
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1"
            }
        )
        # Mask bot detection
        await ctx.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
        page = await ctx.new_page()
        
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=45000)
            await asyncio.sleep(3)

            # 1. SEARCH PHASE (if on a search-capable page)
            for sel in ["input[type='search']", "input[name='q']", "[placeholder*='search' i]"]:
                try:
                    el = page.locator(sel).first
                    if await el.is_visible(timeout=3000):
                        await el.fill(job_title or "Software Developer")
                        await el.press("Enter")
                        await asyncio.sleep(4)
                        break
                except: pass

            # 2. DEEP CRAWL PHASE
            for page_num in range(1, MAX_PAGES + 1):
                # Rate Check
                html = await page.content()
                if "429" in html or "captcha" in html.lower():
                    return jobs, "BLOCKED (429/Captcha)"

                soup = BeautifulSoup(html, "html.parser")
                
                # extraction logic
                page_jobs = []
                if cont_sel:
                    # Specialized extraction using expert knowledge
                    cards = soup.select(cont_sel)
                    for card in cards:
                        t_el = card.select_one(title_sel) if title_sel else card.select_one("a")
                        if not t_el: continue
                        title = t_el.get_text(strip=True)
                        href  = t_el.get("href", "") or t_el.parent.get("href", "")
                        if not title or not href: continue
                        full = urljoin(BASE, href)
                        
                        comp_sel = expert.get("company", "[class*='company'], [class*='employer']")
                        comp_el = card.select_one(comp_sel)
                        company = comp_el.get_text(strip=True) if comp_el else "N/A"
                        
                        if matches_kws(title, kws):
                            page_jobs.append(normalize_job(title, company, full, SOURCE))
                else:
                    # Fallback to structural extraction
                    page_jobs = extract_jobs_html(soup, BASE, SOURCE, kws, search_filtered=True)
                
                new_on_page = 0
                for j in page_jobs:
                    if j["link"] not in seen:
                        seen.add(j["link"])
                        jobs.append(j)
                        new_on_page += 1
                
                print(f"    {SOURCE} expert-p{page_num}: {new_on_page} new")
                
                # 3. PAGINATION/EXHAUSTION LOGIC
                # Look for "Next", "Load More", or Infinite Scroll indicators
                next_btn = (
                    await page.query_selector("a[rel='next']") or 
                    await page.query_selector("button:has-text('Load More')") or
                    await page.query_selector("a:has-text('Növbəti')") or # AZ common
                    await page.query_selector(".pagination .next a")
                )
                
                if next_btn and await next_btn.is_visible():
                    await next_btn.click()
                    await asyncio.sleep(random.uniform(3, 5))
                else:
                    # Scroll as fallback for infinite loaders
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(2)
                    new_html = await page.content()
                    if new_html == html: # No new content loaded
                        break

            return jobs, f"expert: {len(jobs)} collected"

        except Exception as e:
            return jobs, f"expert err: {str(e)[:50]}"
        finally:
            await browser.close()


def scrape_all_sync():
    """Main orchestrator — upgraded to 'Expert Web Automation Engineer' protocol."""
    global _scrape_state

    if _scrape_state["running"]:
        return

    _state_update(running=True, cancel=False, log=[], new_total=0,
                  current_site="", progress=0, total=0)

    s         = get_settings()
    job_title = s.get("job_title", "").strip()
    category  = s.get("category", "Technology")
    kws       = get_kws_for(job_title, category)
    az_active = bool(s.get("az_active", 0))
    gl_active = bool(s.get("global_active", 1))

    all_sites  = load_sites()
    active     = [x for x in all_sites if x.get("active")]

    # Filter by region toggles
    def keep(site):
        is_az = site.get("az_pack") or ".az" in _domain(site["url"])
        if is_az and not az_active:
            return False
        if not is_az and not gl_active:
            return False
        return True

    active = [site for site in active if keep(site)]
    _state_update(total=len(active))

    n_phrases = len(kws.get("phrases", [])) if isinstance(kws, dict) else len(kws)
    print(f"\n[{datetime.now():%H:%M}] Expert Scrape: {len(active)} sites | phrases={n_phrases}")

    # Split into Expert/Isolation sites vs Fast/Parallel sites
    expert_pw_sites = [s for s in active if (s.get("az_pack") or s.get("type") == "playwright")]
    fast_sync_sites = [s for s in active if not (s.get("az_pack") or s.get("type") == "playwright")]

    completed = 0

    # ── Phase 1: Fast Parallel Scrapers (RSS/API) ─────────────────────
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_run_site, site, job_title, kws): site
                   for site in fast_sync_sites}

        for future in as_completed(futures):
            if _scrape_state["cancel"]:
                break
            site = futures[future]
            try:
                jobs, info = future.result()
            except Exception as e:
                jobs, info = [], f"exception: {str(e)[:60]}"

            added = save_jobs_to_db(jobs, kws)
            completed += 1
            _state_update(
                current_site=site["name"],
                progress=completed,
                new_total=_scrape_state["new_total"] + added,
            )
            _scrape_state["log"].append({"site": site["name"], "status": info, "new": added})
            print(f"  [{completed}/{len(active)}] {site['name']}: {info} → +{added}")

    # ── Phase 2: Expert Sequential Scrapers (Isolated Context) ─────────
    for site in expert_pw_sites:
        if _scrape_state["cancel"]:
            break
        
        _state_update(current_site=f"🕵️ Expert: {site['name']}")
        print(f"  [{completed+1}/{len(active)}] {site['name']} (Isolated Protocol)")

        # Fresh context logic is inside _run_site for Playwright
        jobs, info = _run_site(site, job_title, kws)

        # Rate Limit / CAPTCHA Detection
        if any(err in info.upper() for err in ["429", "CAPTCHA", "BLOCKED", "FORBIDDEN"]):
            info = "⚠️ BLOCKED (Rate Limited)"
            # Note: We could mark the site as temporarily inactive here if needed
        
        added = save_jobs_to_db(jobs, kws)
        completed += 1
        _state_update(
            progress=completed,
            new_total=_scrape_state["new_total"] + added,
        )
        _scrape_state["log"].append({"site": site["name"], "status": info, "new": added})
        print(f"     {info} → +{added}")
        time.sleep(random.uniform(2, 4)) # Adaptive delay between site sessions

    _state_update(running=False, current_site="")
    print(f"\n✅ Expert Scan Complete. New jobs: {_scrape_state['new_total']}\n")


def start_scrape_background():
    """Kick off scraping in a daemon thread."""
    if _scrape_state["running"]:
        return False
    t = threading.Thread(target=scrape_all_sync, daemon=True)
    t.start()
    return True


# ═══════════════════════════════════════════════════════════════
#  FLASK ROUTES
# ═══════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return render_template("index.html")

# ── Settings ───────────────────────────────────────────────────
@app.route("/api/settings", methods=["GET"])
def get_user_settings():
    return jsonify(get_settings())

@app.route("/api/settings", methods=["POST"])
def update_settings():
    d             = request.json or {}
    job_title     = d.get("job_title", "").strip()
    job_field     = d.get("job_field", "").strip()
    category      = d.get("category", "Technology").strip()
    az_active     = 1 if d.get("az_active") else 0
    global_active = 1 if d.get("global_active", 1) else 0

    conn = get_db()
    conn.execute(
        "UPDATE settings SET job_title=?,job_field=?,category=?,az_active=?,global_active=? WHERE id=1",
        (job_title, job_field, category, az_active, global_active)
    )
    conn.commit()
    conn.close()

    current = load_sites()
    final   = rebuild_sites(bool(global_active), bool(az_active), current)
    save_sites(final)
    return jsonify({"ok": True})

@app.route("/api/taxonomy")
def get_taxonomy():
    # Return a simplified structure for the frontend: category -> list of role titles
    simplified = {}
    for cat, roles in JOB_TAXONOMY.items():
        simplified[cat] = list(roles.keys())
    return jsonify(simplified)

# ── Jobs ───────────────────────────────────────────────────────
@app.route("/api/jobs")
def get_jobs():
    conn = get_db()
    rows = conn.execute("SELECT * FROM jobs ORDER BY added DESC").fetchall()
    conn.close()
    return jsonify([{
        "id":      r["id"],
        "link":    r["job_url"],
        "source":  r["source"],
        "title":   r["title"],
        "company": r["company"],
        "salary":  r["salary"],
        "date":    r["date"],
        "status":  r["status"],
        "added":   r["added"],
    } for r in rows])

@app.route("/api/jobs/clear", methods=["POST"])
def clear_jobs():
    conn = get_db()
    conn.execute("DELETE FROM jobs")
    conn.commit()
    conn.close()
    return jsonify({"ok": True})

@app.route("/api/jobs/<job_id>/status", methods=["POST"])
def update_status(job_id):
    conn = get_db()
    conn.execute("UPDATE jobs SET status=? WHERE id=?",
                 (request.json.get("status"), job_id))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})

@app.route("/api/jobs/<job_id>", methods=["DELETE"])
def delete_job(job_id):
    conn = get_db()
    conn.execute("DELETE FROM jobs WHERE id=?", (job_id,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})

# ── Scrape control ─────────────────────────────────────────────
@app.route("/api/scrape/start", methods=["POST"])
def scrape_start():
    started = start_scrape_background()
    return jsonify({"ok": started,
                    "message": "Scrape already running" if not started else "Started"})

@app.route("/api/scrape/stop", methods=["POST"])
def scrape_stop():
    _state_update(cancel=True)
    return jsonify({"ok": True})

@app.route("/api/scrape/status", methods=["GET"])
def scrape_status():
    return jsonify(_scrape_state)

# ── Sites ──────────────────────────────────────────────────────
@app.route("/api/sites")
def get_sites():
    return jsonify(load_sites())

@app.route("/api/sites", methods=["POST"])
def add_site():
    d   = request.json or {}
    url = d.get("url", "").strip()
    if not url:
        return jsonify({"ok": False, "error": "URL required"}), 400
    if not url.startswith("http"):
        url = "https://" + url
    name = d.get("name", "").strip() or site_name_from_url(url)
    typ  = d.get("type", "crawler")

    sites = load_sites()
    new_dom = _domain(url)
    for s in sites:
        if _domain(s["url"]) == new_dom:
            return jsonify({"ok": False, "error": f"Already tracking {s['name']}"}), 400

    sites.append({"name": name, "url": url, "active": True, "type": typ})
    save_sites(sites)
    return jsonify({"ok": True, "name": name, "type": typ})

@app.route("/api/sites/quick", methods=["POST"])
def quick_add():
    return add_site()

@app.route("/api/sites/<int:idx>", methods=["DELETE"])
def delete_site(idx):
    s = load_sites()
    if 0 <= idx < len(s):
        s.pop(idx)
        save_sites(s)
    return jsonify({"ok": True})

@app.route("/api/sites/<int:idx>/toggle", methods=["POST"])
def toggle_site(idx):
    s = load_sites()
    if 0 <= idx < len(s):
        s[idx]["active"] = not s[idx]["active"]
        save_sites(s)
    return jsonify({"ok": True})

@app.route("/api/sites/reset", methods=["POST"])
def reset_sites():
    s = get_settings()
    final = rebuild_sites(bool(s.get("global_active", 1)), bool(s.get("az_active", 0)), [])
    save_sites(final)
    return jsonify({"ok": True})

@app.route("/api/sites/<int:idx>/type", methods=["POST"])
def set_site_type(idx):
    s = load_sites()
    if 0 <= idx < len(s):
        s[idx]["type"] = (request.json or {}).get("type", "crawler")
        save_sites(s)
    return jsonify({"ok": True})

@app.route("/api/sites/name", methods=["POST"])
def get_site_name():
    url = (request.json or {}).get("url", "")
    return jsonify({"name": site_name_from_url(url)})

# ── Debug / health ─────────────────────────────────────────────
@app.route("/api/health")
def health():
    conn = get_db()
    job_count = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    conn.close()
    return jsonify({
        "status":    "ok",
        "jobs":      job_count,
        "playwright": PLAYWRIGHT_AVAILABLE,
        "sites":     len(load_sites()),
    })

@app.route("/api/test/<site_name>")
def test_site(site_name):
    """Quick-test a single site scraper. Usage: /api/test/boss.az"""
    sites = load_sites()
    site = next((s for s in sites if site_name.lower() in s["name"].lower()), None)
    if not site:
        return jsonify({"error": "site not found"}), 404
    s_cfg = get_settings()
    kws   = get_kws_for(s_cfg.get("job_title",""), s_cfg.get("category","Technology"))
    jobs, info = _run_site(site, s_cfg.get("job_title",""), kws)
    return jsonify({"site": site["name"], "info": info, "count": len(jobs),
                    "jobs": jobs[:10]})

# ═══════════════════════════════════════════════════════════════
#  SCHEDULER + MAIN
# ═══════════════════════════════════════════════════════════════
scheduler = BackgroundScheduler()

if __name__ == "__main__":
    init_db()
    load_sites()  # ensure sites.json is initialised
    scheduler.add_job(start_scrape_background, "interval", hours=24, id="daily")
    scheduler.start()
    print(f"\n🚀 JobTracker Pro running on http://127.0.0.1:{PORT}")
    print(f"   Playwright: {'✅' if PLAYWRIGHT_AVAILABLE else '❌ (install with: playwright install chromium)'}")
    app.run(host="0.0.0.0", port=PORT, debug=False)