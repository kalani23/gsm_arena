"""
GSMArena Full Scraper v5 - GitHub Actions Mode
===============================================
- No proxies — pure direct connection
- 10 parallel workers
- Exits with code 2 when blocked (10 consecutive fails)
- GitHub Actions triggers new job = new IP = auto resume
- Incremental via seen_ids.json + discovered_devices.json

Usage:
    python gsmarena_scraper_v5.py                    # run
    python gsmarena_scraper_v5.py --workers 10       # 10 parallel workers
    python gsmarena_scraper_v5.py --full-refresh     # rescrape everything
    python gsmarena_scraper_v5.py --sample 50        # test 50 devices
    python gsmarena_scraper_v5.py --brand samsung    # single brand
"""

import re, sys, json, time, random, logging, threading, itertools, io, argparse
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from bs4 import BeautifulSoup
import requests
import pandas as pd

BASE_URL = "https://www.gsmarena.com"

# Force desktop site — mobile UAs can redirect to m.gsmarena.com with different HTML
DESKTOP_REFERER = "https://www.gsmarena.com/"

BLOCK_PHRASES = [
    "check the box if you are human", "request unsuccessful",
    "incapsula", "access denied", "verify you are", "error 15",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

REFERERS = [
    "https://www.gsmarena.com/",
    "https://www.google.com/",
    "https://www.google.com/search?q=gsmarena",
    "https://www.bing.com/",
    "",  # direct
    "",  # direct weighted higher
]

def random_headers():
    ua  = random.choice(USER_AGENTS)
    ref = random.choice(REFERERS)
    h   = {
        "User-Agent":                ua,
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language":           random.choice(["en-US,en;q=0.9", "en-GB,en;q=0.9"]),
        "Accept-Encoding":           "gzip, deflate, br",
        "Connection":                "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control":             "max-age=0",
    }
    if ref:
        h["Referer"] = ref
    return h

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

# ── Worker — pure direct requests, no proxy ───────────────────────────────────
class Worker:
    def __init__(self, worker_id):
        self.worker_id     = worker_id
        self.lock          = threading.Lock()
        self._req_count    = 0
        self.session       = self._new_session()

    def _new_session(self):
        s = requests.Session()
        s.headers.update(random_headers())
        return s

    def get(self, url, retries=3):
        for attempt in range(retries):
            try:
                time.sleep(random.uniform(0.3, 0.8))
                self.session.headers.update(random_headers())
                r = self.session.get(url, timeout=15)

                if r.status_code == 429:
                    log.warning(f"[W{self.worker_id}] 429 — returning None immediately for fresh IP trigger")
                    return None  # immediately signal failure — don't retry, let exit logic handle it

                if r.status_code != 200:
                    log.warning(f"[W{self.worker_id}] HTTP {r.status_code} attempt {attempt+1}")
                    time.sleep(3)
                    continue

                html = r.text
                if any(p in html.lower() for p in BLOCK_PHRASES):
                    log.warning(f"[W{self.worker_id}] Block detected — returning None")
                    return None

                return html

            except Exception as e:
                log.warning(f"[W{self.worker_id}] Error attempt {attempt+1}: {e}")
                time.sleep(2)

        log.error(f"[W{self.worker_id}] All retries failed: {url}")
        return None


# ── state ─────────────────────────────────────────────────────────────────────
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
        except: pass
    return None

def save_discovery_cache(devices):
    data = [{"brand": b, "title": t, "url": u} for b, t, u in devices]
    with open(DISCOVERY_CACHE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    log.info(f"Discovery cache saved: {len(devices)} devices")

def save_discovery_progress(completed, devices):
    with open(DISCOVERY_CACHE + ".partial", "w", encoding="utf-8") as f:
        json.dump({"completed_brands": list(completed),
                   "devices": [{"brand": b, "title": t, "url": u} for b, t, u in devices]}, f, indent=2)

def load_discovery_progress():
    path = DISCOVERY_CACHE + ".partial"
    if Path(path).exists():
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            brands = set(data.get("completed_brands", []))
            devices = [(d["brand"], d["title"], d["url"]) for d in data.get("devices", [])]
            log.info(f"Resuming discovery: {len(brands)} brands done, {len(devices)} devices")
            return brands, devices
        except: pass
    return set(), []


# ── discovery ─────────────────────────────────────────────────────────────────
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
    devices, url, page = [], brand_url, 1
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

def parse_release_year(status, announced):
    for raw in [status, announced]:
        m = re.search(r"\b(20\d{2})\b", raw)
        if m: return m.group(1)
    return ""

def classify_device(model_name, display_size_str, soup):
    name = model_name.lower()
    if any(k in name for k in ["watch", "band", "gear s", "galaxy fit", "amazfit", "mi band", "redmi band"]):
        return "Watch"
    if any(k in name for k in ["tab ", "tablet", " pad", "mediapad", "matebook", "slate", "fire hd", "iconia"]):
        return "Tablet"
    try:
        size = float(display_size_str)
        if size < 2.0: return "Watch"
        if size >= 7.0: return "Tablet"
    except: pass
    sensors = (soup.find(attrs={"data-spec": "sensors"}) or soup.new_tag("x")).get_text(" ", strip=True).lower()
    bat_el  = soup.find(attrs={"data-spec": "batdescription1"})
    bat_raw = bat_el.get_text(" ", strip=True) if bat_el else ""
    bat_m   = re.search(r"([\d]+)\s*mAh", bat_raw)
    if ("ecg" in sensors or "heart rate" in sensors) and bat_m and int(bat_m.group(1)) < 500:
        return "Watch"
    return "Phone"

def extract_all_specs(soup):
    specs, section = {}, ""
    for table in soup.select("#specs-list table"):
        for row in table.select("tr"):
            th = row.find("th")
            if th: section = th.get_text(" ", strip=True)
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

    fixed = {
        "Brand":                brand_name,
        "Model":                model_name,
        "Also Known As":        also_known_as,
        "Device Type":          classify_device(model_name, display_size_in, soup),
        "URL":                  url,
        "Scraped At":           datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "Announced":            announced,
        "Release Year":         parse_release_year(status, announced),
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
        if k not in row: row[k] = v
    return row

def save_outputs(results, base_name):
    df   = pd.DataFrame(results)
    fp   = [c for c in FIXED_COLS   if c in df.columns]
    dp   = [c for c in DERIVED_COLS if c in df.columns and c not in fp]
    rest = sorted([c for c in df.columns if c not in fp and c not in dp])
    df   = df[fp + dp + rest]
    df.to_csv(  f"{base_name}.csv",  index=False, encoding="utf-8-sig")
    df.to_excel(f"{base_name}.xlsx", index=False, engine="openpyxl")
    log.info(f"Saved {len(df)} rows to {base_name}.csv + {base_name}.xlsx")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample",       type=int, default=0)
    parser.add_argument("--brand",        type=str, default="")
    parser.add_argument("--output",       type=str, default="gsmarena_devices")
    parser.add_argument("--workers",      type=int, default=5)
    parser.add_argument("--full-refresh", action="store_true")
    args = parser.parse_args()

    log.info(f"=== GSMArena Scraper v5 | workers={args.workers} | direct connection ===")

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
            log.warning(f"Could not load CSV: {e}")

    workers = {i: Worker(i) for i in range(args.workers)}
    log.info(f"Started {args.workers} workers")

    worker_cycle = itertools.cycle(range(args.workers))
    w0           = workers[0]

    # discovery
    all_devices = None
    if not args.full_refresh and not args.brand and not args.sample:
        all_devices = load_discovery_cache()

    if all_devices is None:
        completed, all_devices = (set(), []) if (args.full_refresh or args.brand) else load_discovery_progress()
        brands = get_all_brands(w0)
        if not brands:
            log.error("Could not fetch brand list.")
            sys.exit(1)
        if args.brand:
            brands = [(n, u) for n, u in brands if args.brand.lower() in n.lower()]
        brands_to_do = [(n, u) for n, u in brands if n not in completed]
        log.info(f"Discovery: {len(brands_to_do)} brands remaining")
        for brand_name, brand_url in brands_to_do:
            devs = get_devices_for_brand(w0, brand_name, brand_url)
            all_devices.extend(devs)
            completed.add(brand_name)
            save_discovery_progress(completed, all_devices)
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

    results        = list(existing)
    results_lock   = threading.Lock()
    errors         = []
    errors_lock    = threading.Lock()
    counter        = [0]
    counter_lock   = threading.Lock()
    consec_fails   = [0]
    consec_lock    = threading.Lock()
    FAIL_THRESHOLD = 10
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
                    consec_fails[0] = 0
                with results_lock:
                    results.append(row)
                with _seen_lock:
                    seen.add(device_url)
                log.info(f"[{n}/{total}] OK  {brand_name} - {device_title}")
                if n % 100 == 0:
                    with results_lock:
                        save_outputs(results, args.output)
                    save_seen(seen, seen_path)
                    log.info(f"Checkpoint {n} saved")
            else:
                with errors_lock:
                    errors.append(device_url)
                with consec_lock:
                    consec_fails[0] += 1
                    if consec_fails[0] >= FAIL_THRESHOLD:
                        log.warning(f"{FAIL_THRESHOLD} consecutive failures — saving and exiting for fresh IP")
                        with results_lock:
                            save_outputs(results, args.output)
                        save_seen(seen, seen_path)
                        sys.exit(2)
        except Exception as e:
            with errors_lock:
                errors.append(device_url)
            log.error(f"ERROR {device_url}: {e}")

    log.info(f"Scraping {total} devices with {args.workers} workers...")
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(scrape_task, next(worker_cycle), b, t, u)
            for b, t, u in new_devices
        ]
        for f in as_completed(futures):
            pass

    save_outputs(results, args.output)
    save_seen(seen, seen_path)
    log.info(f"=== Done === New: {len(results) - len(existing)} | Errors: {len(errors)}")


if __name__ == "__main__":
    main()
