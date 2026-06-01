"""
GSMArena Full Scraper v5 - FAST PURE REQUESTS MODE
====================================================
- 100 Webshare proxies with smart rotation
- Blocked proxies get marked and skipped
- Only reuse blocked proxies when all others are exhausted
- Pure requests — no browser, no dialogs
- Parallel workers, incremental runs, discovery cache

Usage:
    python gsmarena_scraper_v5.py                    # incremental run
    python gsmarena_scraper_v5.py --full-refresh     # re-scrape everything
    python gsmarena_scraper_v5.py --sample 50        # test 50 devices
    python gsmarena_scraper_v5.py --brand samsung    # single brand
    python gsmarena_scraper_v5.py --workers 10       # parallel workers
    python gsmarena_scraper_v5.py --output myfile    # custom output name
"""

import re, sys, json, time, random, logging, threading, itertools, io, argparse
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from bs4 import BeautifulSoup
import requests
import pandas as pd

# ── 100 Webshare proxies ──────────────────────────────────────────────────────
WEBSHARE_USER = "aumydtff"
WEBSHARE_PASS = "g1qwkqjrck9r"

WEBSHARE_PROXIES = [
    ("23.95.255.179","6763"),("92.112.235.169","6692"),("166.0.102.206","5445"),
    ("92.113.232.126","7710"),("208.70.8.73","7173"),("216.173.76.143","6770"),
    ("46.203.206.166","5611"),("82.21.224.213","6569"),("82.29.229.222","6577"),
    ("104.238.7.123","6050"),("191.101.25.67","6464"),("46.203.96.13","6137"),
    ("82.22.245.224","6048"),("82.23.225.155","8006"),("92.113.83.130","6075"),
    ("23.94.246.182","8135"),("64.137.83.113","6053"),("82.29.214.122","6973"),
    ("154.36.110.91","6745"),("82.24.232.151","6980"),("107.181.142.199","5792"),
    ("145.223.57.237","6270"),("166.88.155.58","6217"),("194.113.119.76","6750"),
    ("82.23.239.145","6482"),("104.252.41.42","6979"),("152.232.116.181","8723"),
    ("45.150.23.122","6592"),("188.215.5.193","5223"),("104.239.90.249","6640"),
    ("104.249.55.126","6494"),("31.58.24.43","6114"),("45.67.0.247","6683"),
    ("142.147.244.104","6348"),("172.84.181.241","7219"),("82.21.226.90","7403"),
    ("23.229.110.85","8613"),("45.14.83.176","8154"),("82.22.217.46","5388"),
    ("82.22.224.65","7397"),("92.112.91.65","6300"),("148.135.151.77","8328"),
    ("23.94.138.116","6390"),("104.239.35.91","5773"),("92.112.217.133","5905"),
    ("142.111.113.189","6550"),("82.22.210.197","8039"),("104.165.189.219","6874"),
    ("140.99.203.187","6064"),("148.135.191.187","5746"),("152.232.14.199","7330"),
    ("173.0.10.80","6256"),("216.173.80.186","6443"),("154.6.59.5","6473"),
    ("104.222.185.159","5722"),("172.120.101.118","6297"),("184.174.46.104","5733"),
    ("45.150.23.16","6486"),("92.112.137.15","5958"),("45.39.17.129","5552"),
    ("92.112.95.157","6892"),("104.222.185.112","5675"),("107.173.36.182","5637"),
    ("82.26.213.13","5842"),("82.26.221.190","5531"),("188.213.1.213","7119"),
    ("192.186.151.229","8730"),("142.202.253.100","5775"),("148.135.148.28","8025"),
    ("154.6.129.4","5474"),("92.112.236.117","6549"),("104.239.106.114","5759"),
    ("205.164.57.183","5758"),("82.23.202.26","6878"),("206.206.64.30","5991"),
    ("23.129.252.120","6388"),("38.154.195.91","9179"),("45.38.101.158","6091"),
    ("45.39.115.116","5527"),("45.83.57.212","6729"),("64.137.37.67","6657"),
    ("108.165.161.175","5916"),("46.202.79.43","7053"),("64.137.14.235","5901"),
    ("194.38.18.223","7285"),("198.46.246.226","6850"),("45.91.166.139","7198"),
    ("94.177.49.113","6129"),("142.111.124.45","6065"),("64.137.93.94","6551"),
    ("108.165.227.10","5251"),("193.42.225.248","6739"),("23.94.138.51","6325"),
    ("2.57.21.160","7397"),("104.239.37.133","5785"),("193.187.114.253","6268"),
    ("104.239.43.153","5881"),("104.253.66.221","5657"),("108.165.161.159","5900"),
    ("45.39.5.32","6470"),
]

BASE_URL = "https://www.gsmarena.com"

BLOCK_PHRASES = [
    "check the box if you are human", "request unsuccessful",
    "incapsula", "access denied", "verify you are", "error 15",
]

# ── Rotating user agents — 20 real browser fingerprints ──────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; SM-A546B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 OPR/109.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

# Referers to rotate — looks like natural browsing
REFERERS = [
    "https://www.gsmarena.com/",
    "https://www.google.com/search?q=gsmarena",
    "https://www.google.com/",
    "https://www.bing.com/search?q=gsmarena",
    "https://search.yahoo.com/",
    "https://www.gsmarena.com/search.php3",
    "https://www.gsmarena.com/makers.php3",
    "",  # direct navigation
    "",
    "",  # weight direct navigation higher
]

# Desktop and mobile base URLs — alternate between them
DESKTOP_BASE = "https://www.gsmarena.com"
MOBILE_BASE  = "https://www.gsmarena.com"  # same domain, mobile triggered by UA

def get_random_headers():
    """Return a fresh randomised header set per request."""
    ua = random.choice(USER_AGENTS)
    is_mobile = "Mobile" in ua or "iPhone" in ua or "Android" in ua

    headers = {
        "User-Agent":                ua,
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language":           random.choice(["en-US,en;q=0.9", "en-GB,en;q=0.9", "en-US,en;q=0.8,es;q=0.5"]),
        "Accept-Encoding":           "gzip, deflate, br",
        "Connection":                "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest":            "document",
        "Sec-Fetch-Mode":            "navigate",
        "Sec-Fetch-Site":            random.choice(["none", "same-origin", "cross-site"]),
        "Sec-Fetch-User":            "?1",
        "Cache-Control":             random.choice(["max-age=0", "no-cache"]),
    }
    # add referer sometimes (not always — direct navigation is normal)
    ref = random.choice(REFERERS)
    if ref:
        headers["Referer"] = ref

    # DNT header sometimes
    if random.random() < 0.3:
        headers["DNT"] = "1"

    return headers

HEADERS = get_random_headers()  # default fallback

# ── logging ───────────────────────────────────────────────────────────────────
_stream = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace") \
          if hasattr(sys.stdout, "buffer") else sys.stdout
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("gsmarena_run.log", encoding="utf-8"),
        logging.StreamHandler(_stream),
    ],
)
log = logging.getLogger(__name__)

# ── column order ──────────────────────────────────────────────────────────────
FIXED_COLS = [
    "Brand", "Model", "Also Known As", "Device Type",
    "URL", "Scraped At", "Announced", "Release Year", "Status",
]
DERIVED_COLS = [
    "Length mm", "Width mm", "Height mm", "Weight g",
    "Display Size inches", "Resolution Width px", "Resolution Height px", "PPI",
    "Storage Options", "RAM Options",
    "Main Camera MP", "Selfie Camera MP", "Battery mAh",
    "SAR US Head W/kg", "SAR US Body W/kg",
    "SAR EU Head W/kg", "SAR EU Body W/kg",
    "Price USD", "Price EUR", "Price GBP", "Price INR", "Price Raw",
]

# ── smart proxy pool ──────────────────────────────────────────────────────────
PROXY_COOLDOWN_SECONDS = 60  # 5 minutes before a blocked proxy re-enters the pool

class ProxyPool:
    """
    Cooldown-based proxy rotation:
    - Blocked proxies go into a cooldown queue with a timestamp
    - After PROXY_COOLDOWN_SECONDS they automatically return to active pool
    - No manual intervention needed — pool self-heals continuously
    - Thread-safe
    """
    def __init__(self):
        self._lock     = threading.Lock()
        self._active   = list(WEBSHARE_PROXIES)
        self._cooldown = {}  # proxy -> unblock_time
        random.shuffle(self._active)
        log.info(f"Proxy pool ready: {len(self._active)} proxies | cooldown={PROXY_COOLDOWN_SECONDS}s")
        # background thread to restore cooled-down proxies
        t = threading.Thread(target=self._restore_loop, daemon=True)
        t.start()

    def _restore_loop(self):
        """Background thread — checks every 30s and restores cooled proxies."""
        while True:
            time.sleep(30)
            now = time.time()
            with self._lock:
                restored = [p for p, unblock_at in self._cooldown.items() if now >= unblock_at]
                for p in restored:
                    del self._cooldown[p]
                    self._active.append(p)
                if restored:
                    log.info(f"Proxy pool: restored {len(restored)} proxies | Active: {len(self._active)} | Cooling: {len(self._cooldown)}")

    def get(self):
        with self._lock:
            if not self._active:
                # all in cooldown — find the one closest to unblocking
                if self._cooldown:
                    soonest = min(self._cooldown, key=self._cooldown.get)
                    wait    = max(0, self._cooldown[soonest] - time.time())
                    log.warning(f"All proxies cooling down — waiting {wait:.0f}s for next available")
                    # release lock while waiting
                    unblock_time = self._cooldown[soonest]
                else:
                    return None, None
            if self._active:
                host, port = random.choice(self._active)
                return host, port
            # wait outside lock for cooldown
            return None, None

    def get_blocking(self):
        """Like get() but waits until a proxy is available if pool is empty."""
        while True:
            with self._lock:
                # restore any expired cooldowns first
                now      = time.time()
                restored = [p for p, t in self._cooldown.items() if now >= t]
                for p in restored:
                    del self._cooldown[p]
                    self._active.append(p)
                if self._active:
                    host, port = random.choice(self._active)
                    return host, port
                # find shortest wait
                if self._cooldown:
                    wait = max(5, min(self._cooldown.values()) - now)
                else:
                    wait = 5
            log.warning(f"All proxies cooling — waiting {wait:.0f}s")
            time.sleep(wait)

    def mark_blocked(self, host, port):
        with self._lock:
            entry = (host, port)
            if entry in self._active:
                self._active.remove(entry)
            unblock_at = time.time() + PROXY_COOLDOWN_SECONDS
            self._cooldown[entry] = unblock_at
            log.warning(f"Proxy cooling: {host}:{port} | Active: {len(self._active)} | Cooling: {len(self._cooldown)} | Unblocks in {PROXY_COOLDOWN_SECONDS}s")

    def status(self):
        with self._lock:
            return len(self._active), len(self._cooldown)


PROXY_POOL = ProxyPool()


def make_session(host, port):
    s = requests.Session()
    proxy_url = f"http://{WEBSHARE_USER}:{WEBSHARE_PASS}@{host}:{port}"
    s.proxies = {"http": proxy_url, "https": proxy_url}
    s.headers.update(get_random_headers())
    return s


# ── Worker ────────────────────────────────────────────────────────────────────
class Worker:
    def __init__(self, worker_id, no_proxy=False):
        self.worker_id   = worker_id
        self.no_proxy    = no_proxy
        self.lock        = threading.Lock()
        self.proxy_host  = None
        self.proxy_port  = None
        self.session     = None
        self._request_count = 0
        self._rotate()

    def _make_session(self):
        s = requests.Session()
        if not self.no_proxy:
            proxy_url = f"http://{WEBSHARE_USER}:{WEBSHARE_PASS}@{self.proxy_host}:{self.proxy_port}"
            s.proxies = {"http": proxy_url, "https": proxy_url}
        s.headers.update(get_random_headers())
        return s

    def _rotate(self):
        if self.no_proxy:
            self.proxy_host = "direct"
            self.proxy_port = "0"
        else:
            host, port = PROXY_POOL.get_blocking()
            self.proxy_host = host
            self.proxy_port = port
        self.session        = self._make_session()
        self._request_count = 0

    def _maybe_refresh_session(self):
        """Every 50-80 requests, do a homepage visit to look like natural browsing."""
        if not hasattr(self, "_request_count"):
            self._request_count = 0
        self._request_count += 1
        threshold = random.randint(50, 80)
        if self._request_count >= threshold:
            self._request_count = 0
            try:
                self.session.headers.update(get_random_headers())
                self.session.get(BASE_URL, timeout=10)
                time.sleep(random.uniform(1.0, 2.0))
                log.info(f"  [W{self.worker_id}] Session refresh (homepage visit)")
            except: pass

    def warm_up(self):
        # No pre-flight requests — just verify proxy is connectable
        # GSMArena does not require cookie harvesting, requests work directly
        log.info(f"  [W{self.worker_id}] Ready on {self.proxy_host}:{self.proxy_port}")
        return True

    def get(self, url, retries=5):
        for attempt in range(retries):
            try:
                # randomise delay — longer in no-proxy mode to avoid rate limits
                delay = random.uniform(0.5, 1.5)
                time.sleep(delay)
                # rotate headers on every request
                self.session.headers.update(get_random_headers())
                # occasionally visit homepage to reset session fingerprint
                self._maybe_refresh_session()
                r = self.session.get(url, timeout=15)

                if r.status_code == 429:
                    active, blocked = PROXY_POOL.status()
                    log.warning(f"  [W{self.worker_id}] 429 on {self.proxy_host}:{self.proxy_port} | Pool: {active} active / {blocked} blocked")
                    PROXY_POOL.mark_blocked(self.proxy_host, self.proxy_port)
                    self._rotate()
                    wait = random.uniform(10.0, 20.0)
                    log.info(f"  [W{self.worker_id}] Waiting {wait:.0f}s before retry on new proxy")
                    time.sleep(wait)
                    continue

                if r.status_code != 200:
                    log.warning(f"  [W{self.worker_id}] HTTP {r.status_code} attempt {attempt+1}")
                    time.sleep(5)
                    continue

                html = r.text
                if any(p in html.lower() for p in BLOCK_PHRASES):
                    log.warning(f"  [W{self.worker_id}] Block detected — rotating proxy")
                    PROXY_POOL.mark_blocked(self.proxy_host, self.proxy_port)
                    self._rotate()
                    self.warm_up()
                    continue

                return html

            except Exception as e:
                err_str = str(e).lower()
                if "proxy" in err_str or "remote end closed" in err_str or "connection" in err_str:
                    log.warning(f"  [W{self.worker_id}] Proxy connection error — rotating: {e}")
                    PROXY_POOL.mark_blocked(self.proxy_host, self.proxy_port)
                    self._rotate()
                else:
                    log.warning(f"  [W{self.worker_id}] Error attempt {attempt+1}: {e}")
                time.sleep(2)

        log.error(f"  [W{self.worker_id}] All retries failed: {url}")
        return None

    def quit(self):
        pass


# ── incremental + discovery state ─────────────────────────────────────────────
_seen_lock      = threading.Lock()
DISCOVERY_CACHE = "discovered_devices.json"

def load_seen(path="seen_ids.json"):
    if Path(path).exists():
        with open(path, encoding="utf-8") as f:
            return set(json.load(f))
    return set()

def save_seen(seen, path="seen_ids.json"):
    with _seen_lock:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(list(seen), f)

def load_discovery_cache():
    if Path(DISCOVERY_CACHE).exists():
        try:
            with open(DISCOVERY_CACHE, encoding="utf-8") as f:
                data = json.load(f)
            log.info(f"Loaded discovery cache: {len(data)} devices")
            return [(d["brand"], d["title"], d["url"]) for d in data]
        except Exception as e:
            log.warning(f"Could not load discovery cache: {e}")
    return None

def save_discovery_cache(devices):
    data = [{"brand": b, "title": t, "url": u} for b, t, u in devices]
    with open(DISCOVERY_CACHE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    log.info(f"Discovery cache saved: {len(devices)} devices")

def save_discovery_progress(completed_brands, devices_so_far):
    data = {
        "completed_brands": list(completed_brands),
        "devices": [{"brand": b, "title": t, "url": u} for b, t, u in devices_so_far]
    }
    with open(DISCOVERY_CACHE + ".partial", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

def load_discovery_progress():
    path = DISCOVERY_CACHE + ".partial"
    if Path(path).exists():
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            brands_done = set(data.get("completed_brands", []))
            devices     = [(d["brand"], d["title"], d["url"]) for d in data.get("devices", [])]
            log.info(f"Resuming discovery: {len(brands_done)} brands done, {len(devices)} devices so far")
            return brands_done, devices
        except Exception as e:
            log.warning(f"Could not load partial discovery: {e}")
    return set(), []


# ── brand / device discovery ──────────────────────────────────────────────────
def get_all_brands(worker):
    html = worker.get(f"{BASE_URL}/makers.php3")
    if not html: return []
    soup   = BeautifulSoup(html, "html.parser")
    brands = []
    for a in soup.select("div.st-text td a"):
        name = a.get_text(strip=True)
        name = re.sub(r"\s*\(.*?\)", "", name).strip()
        name = re.sub(r"\d+\s*devices?\s*$", "", name, flags=re.IGNORECASE).strip()
        brands.append((name, f"{BASE_URL}/{a['href']}"))
    log.info(f"Found {len(brands)} brands")
    return brands

def get_devices_for_brand(worker, brand_name, brand_url):
    devices = []
    url     = brand_url
    page    = 1
    while url:
        html = worker.get(url)
        if not html: break
        soup = BeautifulSoup(html, "html.parser")
        for li in soup.select("div.makers ul li"):
            a = li.find("a")
            if a:
                title = a.get("title", "") or a.get_text(strip=True)
                devices.append((brand_name, title, f"{BASE_URL}/{a['href']}"))
        next_a = soup.select_one("a.prevnextbutton[title='Next page']")
        if next_a:
            url = f"{BASE_URL}/{next_a['href']}"
            page += 1
        else:
            break
    log.info(f"  {brand_name}: {len(devices)} devices ({page} pages)")
    return devices


# ── parsers ───────────────────────────────────────────────────────────────────
def parse_dimensions(raw):
    m = re.search(r"([\d.]+)\s*x\s*([\d.]+)\s*x\s*([\d.]+)\s*mm", raw)
    return (m.group(1), m.group(2), m.group(3)) if m else ("", "", "")

def parse_weight(raw):
    m = re.search(r"([\d.]+)\s*g\b", raw)
    return m.group(1) if m else ""

def parse_display_size(raw):
    m = re.search(r"([\d.]+)\s*inch", raw)
    return m.group(1) if m else ""

def parse_resolution(raw):
    m   = re.search(r"([\d]+)\s*x\s*([\d]+)\s*pixels", raw)
    ppi = re.search(r"~?([\d]+)\s*ppi", raw)
    return (m.group(1) if m else "", m.group(2) if m else "", ppi.group(1) if ppi else "")

def parse_battery(raw):
    m = re.search(r"([\d]+)\s*mAh", raw)
    return m.group(1) if m else ""

def parse_memory(raw):
    storages, rams = set(), set()
    for chunk in raw.split(","):
        chunk = chunk.strip()
        sm = re.search(r"^([\d]+(?:TB|GB|MB))", chunk)
        r  = re.search(r"([\d]+(?:TB|GB|MB))\s+RAM", chunk)
        if sm: storages.add(sm.group(1))
        if r:  rams.add(r.group(1))
    return " | ".join(sorted(storages)), " | ".join(sorted(rams))

def parse_camera_mp(raw):
    m = re.search(r"([\d]+)\s*MP", raw)
    return m.group(1) if m else ""

def parse_sar(raw, part):
    m = re.search(r"([\d.]+)\s*W/kg\s*\(" + part + r"\)", raw, re.IGNORECASE)
    return m.group(1) if m else ""

def parse_price(raw, symbol):
    m = re.search(re.escape(symbol) + r"\s*[\u202f\u00a0 ]?([\d,]+(?:\.\d+)?)", raw)
    return m.group(1).replace(",", "") if m else ""

def parse_release_year(status_raw, announced_raw):
    for raw in [status_raw, announced_raw]:
        m = re.search(r"\b(20\d{2})\b", raw)
        if m: return m.group(1)
    return ""

def classify_device(model_name, display_size_str, soup):
    name = model_name.lower()
    if any(k in name for k in ["watch", "band", "gear s", "galaxy fit",
                                "amazfit", "mi band", "redmi band"]):
        return "Watch"
    if any(k in name for k in ["tab ", "tablet", " pad", "mediapad",
                                "matebook", "slate", "fire hd", "iconia"]):
        return "Tablet"
    try:
        size = float(display_size_str)
        if size < 2.0: return "Watch"
        if size >= 7.0: return "Tablet"
    except (ValueError, TypeError): pass
    sensors = (soup.find(attrs={"data-spec": "sensors"}) or
               soup.new_tag("x")).get_text(" ", strip=True).lower()
    bat_el  = soup.find(attrs={"data-spec": "batdescription1"})
    bat_raw = bat_el.get_text(" ", strip=True) if bat_el else ""
    bat_m   = re.search(r"([\d]+)\s*mAh", bat_raw)
    if ("ecg" in sensors or "heart rate" in sensors) and bat_m and int(bat_m.group(1)) < 500:
        return "Watch"
    return "Phone"

def extract_all_specs(soup):
    specs   = {}
    section = ""
    for table in soup.select("#specs-list table"):
        for row in table.select("tr"):
            th = row.find("th")
            if th:
                section = th.get_text(" ", strip=True)
            ttl    = row.find("td", class_="ttl")
            nfo_td = row.find("td", class_="nfo")
            if not ttl or not nfo_td: continue
            label = ttl.get_text(" ", strip=True).rstrip("?").strip()
            value = nfo_td.get_text(" ", strip=True)
            col   = f"{section}_{label}" if section and label else (section or label)
            if not col.strip(): continue
            if col in specs:
                i = 2
                while f"{col}_{i}" in specs: i += 1
                col = f"{col}_{i}"
            specs[col] = value
    return specs


# ── scrape single device ──────────────────────────────────────────────────────
def scrape_device(worker, brand_name, device_title, url):
    html = worker.get(url)
    if not html: return None

    soup          = BeautifulSoup(html, "html.parser")
    h1            = soup.find("h1", class_="specs-phone-name-title")
    model_name    = h1.get_text(strip=True) if h1 else device_title
    note          = soup.find("p", attrs={"data-spec": "comment"})
    also_known_as = note.get_text(strip=True) if note else ""
    dynamic       = extract_all_specs(soup)

    dim_raw   = dynamic.get("Body_Dimensions", "")
    wt_raw    = dynamic.get("Body_Weight", "")
    ds_raw    = dynamic.get("Display_Size", "")
    dr_raw    = dynamic.get("Display_Resolution", "")
    mem_raw   = dynamic.get("Memory_Internal", "")
    c1_raw    = (dynamic.get("Main Camera_Triple") or dynamic.get("Main Camera_Dual") or
                 dynamic.get("Main Camera_Single") or dynamic.get("Main Camera_Quad") or "")
    c2_raw    = dynamic.get("Selfie camera_Single") or dynamic.get("Selfie camera_Dual") or ""
    bat_raw   = dynamic.get("Battery_Type", "")
    sar_us    = dynamic.get("Misc_SAR", "") or dynamic.get("Misc_SAR US", "")
    sar_eu    = dynamic.get("Misc_SAR EU", "")
    price_raw = dynamic.get("Misc_Price", "")
    status    = dynamic.get("Launch_Status", "")
    announced = dynamic.get("Launch_Announced", "")

    dim_l, dim_w, dim_h   = parse_dimensions(dim_raw)
    weight_g               = parse_weight(wt_raw)
    display_size_in        = parse_display_size(ds_raw)
    disp_w, disp_h, ppi   = parse_resolution(dr_raw)
    storage_opts, ram_opts = parse_memory(mem_raw)
    bat_mah                = parse_battery(bat_raw)
    release_year           = parse_release_year(status, announced)
    device_type            = classify_device(model_name, display_size_in, soup)

    fixed = {
        "Brand":                brand_name,
        "Model":                model_name,
        "Also Known As":        also_known_as,
        "Device Type":          device_type,
        "URL":                  url,
        "Scraped At":           datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "Announced":            announced,
        "Release Year":         release_year,
        "Status":               status,
        "Length mm":            dim_l,
        "Width mm":             dim_w,
        "Height mm":            dim_h,
        "Weight g":             weight_g,
        "Display Size inches":  display_size_in,
        "Resolution Width px":  disp_w,
        "Resolution Height px": disp_h,
        "PPI":                  ppi,
        "Storage Options":      storage_opts,
        "RAM Options":          ram_opts,
        "Main Camera MP":       parse_camera_mp(c1_raw),
        "Selfie Camera MP":     parse_camera_mp(c2_raw),
        "Battery mAh":          bat_mah,
        "SAR US Head W/kg":     parse_sar(sar_us, "head"),
        "SAR US Body W/kg":     parse_sar(sar_us, "body"),
        "SAR EU Head W/kg":     parse_sar(sar_eu, "head"),
        "SAR EU Body W/kg":     parse_sar(sar_eu, "body"),
        "Price USD":            parse_price(price_raw, "$"),
        "Price EUR":            parse_price(price_raw, "€"),
        "Price GBP":            parse_price(price_raw, "£"),
        "Price INR":            parse_price(price_raw, "₹"),
        "Price Raw":            price_raw,
    }

    row = {**fixed}
    for k, v in dynamic.items():
        if k not in row:
            row[k] = v
    return row


# ── save ──────────────────────────────────────────────────────────────────────
def save_outputs(results, base_name):
    df   = pd.DataFrame(results)
    fp   = [c for c in FIXED_COLS   if c in df.columns]
    dp   = [c for c in DERIVED_COLS if c in df.columns and c not in fp]
    rest = sorted([c for c in df.columns if c not in fp and c not in dp])
    df   = df[fp + dp + rest]
    df.to_csv(  f"{base_name}.csv",  index=False, encoding="utf-8-sig")
    df.to_excel(f"{base_name}.xlsx", index=False, engine="openpyxl")
    log.info(f"Saved {len(df)} rows to {base_name}.csv + {base_name}.xlsx")
    return df


# ── main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="GSMArena scraper v5")
    parser.add_argument("--sample",       type=int, default=0)
    parser.add_argument("--brand",        type=str, default="")
    parser.add_argument("--output",       type=str, default="gsmarena_devices")
    parser.add_argument("--workers",      type=int, default=3,
                        help="Parallel workers (default 10, safe with 100 proxies)")
    parser.add_argument("--full-refresh", action="store_true")
    parser.add_argument("--no-proxy",     action="store_true",
                        help="Skip proxies — direct connection, 1 worker, safe overnight mode")
    args = parser.parse_args()

    log.info(f"=== GSMArena Scraper v5 | workers={args.workers} | proxies=100 ===")

    seen      = set() if args.full_refresh else load_seen()
    seen_path = "seen_ids.json"
    log.info(f"Already scraped: {len(seen)} | full_refresh={args.full_refresh}")

    existing = []
    csv_path = f"{args.output}.csv"
    if Path(csv_path).exists() and not args.full_refresh:
        try:
            existing = pd.read_csv(csv_path, encoding="utf-8-sig").to_dict("records")
            log.info(f"Loaded {len(existing)} existing rows")
        except Exception as e:
            log.warning(f"Could not load existing CSV: {e}")

    if getattr(args, 'no_proxy', False):
        log.info(f"NO-PROXY MODE — direct connection, {args.workers} workers, 1.5-3s delay")

    log.info(f"Starting {args.workers} workers...")
    workers = {}
    for i in range(args.workers):
        w = Worker(i, no_proxy=getattr(args, 'no_proxy', False))
        w.warm_up()
        workers[i] = w

    worker_cycle = itertools.cycle(range(args.workers))
    w0           = workers[0]

    # discovery
    all_devices = None
    if not args.full_refresh and not args.brand and not args.sample:
        all_devices = load_discovery_cache()

    if all_devices is None:
        completed_brands, all_devices = (
            (set(), []) if (args.full_refresh or args.brand)
            else load_discovery_progress()
        )
        brands = get_all_brands(w0)
        if not brands:
            log.error("Could not fetch brand list. Exiting.")
            sys.exit(1)

        if args.brand:
            brands = [(n, u) for n, u in brands if args.brand.lower() in n.lower()]

        brands_to_do = [(n, u) for n, u in brands if n not in completed_brands]
        log.info(f"Discovery: {len(brands_to_do)} brands remaining")

        for brand_name, brand_url in brands_to_do:
            devs = get_devices_for_brand(w0, brand_name, brand_url)
            all_devices.extend(devs)
            completed_brands.add(brand_name)
            save_discovery_progress(completed_brands, all_devices)
            if args.sample and len(all_devices) >= args.sample:
                all_devices = all_devices[:args.sample]
                break

        if not args.brand and not args.sample:
            save_discovery_cache(all_devices)
            try: Path(DISCOVERY_CACHE + ".partial").unlink()
            except: pass
    else:
        log.info("Using cached device list")

    new_devices = [(b, t, u) for b, t, u in all_devices if u not in seen]
    log.info(f"Total: {len(all_devices)} | New: {len(new_devices)}")

    if not new_devices:
        log.info("Nothing new to scrape.")
        return

    results      = list(existing)
    results_lock = threading.Lock()
    errors       = []
    errors_lock  = threading.Lock()
    counter        = [0]
    counter_lock   = threading.Lock()
    consec_fails   = [0]          # consecutive 429/fail counter
    consec_lock    = threading.Lock()
    FAIL_THRESHOLD = 30           # exit and let GH Actions retry with fresh IP
    total          = len(new_devices)

    def scrape_task(worker_id, brand_name, device_title, device_url):
        worker = workers[worker_id]
        try:
            row = scrape_device(worker, brand_name, device_title, device_url)
            with counter_lock:
                counter[0] += 1
                n = counter[0]
            if row:
                with consec_lock:
                    consec_fails[0] = 0  # reset on success
                with results_lock:
                    results.append(row)
                with _seen_lock:
                    seen.add(device_url)
                log.info(f"[{n}/{total}] OK  {brand_name} - {device_title}")
                if n % 100 == 0:
                    with results_lock:
                        save_outputs(results, args.output)
                    save_seen(seen, seen_path)
                    active, blocked = PROXY_POOL.status()
                    log.info(f"Checkpoint {n} done | Proxies: {active} active / {blocked} cooling")
            else:
                with errors_lock:
                    errors.append(device_url)
                with consec_lock:
                    consec_fails[0] += 1
                    if consec_fails[0] >= FAIL_THRESHOLD:
                        log.warning(f"{FAIL_THRESHOLD} consecutive failures — saving progress and exiting for fresh IP retry")
                        with results_lock:
                            save_outputs(results, args.output)
                        save_seen(seen, seen_path)
                        sys.exit(2)  # exit code 2 = blocked, retry needed
        except Exception as e:
            with errors_lock:
                errors.append(device_url)
            log.error(f"ERROR {device_url}: {e}")

    log.info(f"Scraping {total} devices with {args.workers} workers + 100 proxies...")
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(scrape_task, next(worker_cycle), b, t, u)
            for b, t, u in new_devices
        ]
        for f in as_completed(futures):
            pass

    save_outputs(results, args.output)
    save_seen(seen, seen_path)

    active, blocked = PROXY_POOL.status()
    log.info("=== Done ===")
    log.info(f"New scraped: {len(results) - len(existing)} | Errors: {len(errors)}")
    log.info(f"Final proxy status: {active} active / {blocked} cooling")
    if errors:
        for u in errors:
            log.warning(f"  FAILED: {u}")


if __name__ == "__main__":
    main()
