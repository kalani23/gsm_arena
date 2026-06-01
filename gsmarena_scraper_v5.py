"""
GSMArena Scraper v5 - Chunk mode
- Reads CHUNK_ID, CHUNK_START, CHUNK_END from environment
- Scrapes only its assigned slice of devices
- On first 429 -> saves progress -> exits code 2
- Workflow triggers NEW job for same chunk with fresh IP
- Resumes from seen_ids.json automatically
"""

import re, sys, os, json, time, random, logging, threading, io
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import requests
import pandas as pd

BASE_URL = "https://www.gsmarena.com"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
]

def random_headers():
    return {
        "User-Agent":      random.choice(USER_AGENTS),
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection":      "keep-alive",
        "Cache-Control":   "max-age=0",
    }

# ── logging ───────────────────────────────────────────────────────────────────
_stream = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace") \
          if hasattr(sys.stdout, "buffer") else sys.stdout
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    handlers=[logging.StreamHandler(_stream)],
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

# ── HTTP ──────────────────────────────────────────────────────────────────────
def make_session():
    s = requests.Session()
    s.headers.update(random_headers())
    return s

def fetch(session, url):
    try:
        session.headers.update(random_headers())
        r = session.get(url, timeout=15)
        if r.status_code == 429:
            return "BLOCKED"
        if r.status_code != 200:
            return None
        if any(p in r.text.lower() for p in ["incapsula", "access denied", "check the box"]):
            return "BLOCKED"
        return r.text
    except Exception as e:
        log.warning(f"Error: {e}")
        return None

# ── trigger new job for same chunk with fresh IP ──────────────────────────────
def trigger_resume(chunk_id, chunk_start, chunk_end):
    pat  = os.environ.get("PAT_TOKEN", "")
    repo = os.environ.get("REPO", "")
    if not pat or not repo:
        log.warning("No PAT_TOKEN/REPO env — cannot trigger resume")
        return
    try:
        import urllib.request
        payload = json.dumps({
            "event_type": "scrape_chunk",
            "client_payload": {
                "chunk_id":    str(chunk_id),
                "chunk_start": str(chunk_start),
                "chunk_end":   str(chunk_end),
            }
        }).encode()
        req = urllib.request.Request(
            f"https://api.github.com/repos/{repo}/dispatches",
            data=payload,
            headers={
                "Accept":        "application/vnd.github.v3+json",
                "Authorization": f"token {pat}",
                "Content-Type":  "application/json",
            },
            method="POST"
        )
        urllib.request.urlopen(req)
        log.info(f"Triggered resume for chunk {chunk_id} ({chunk_start}-{chunk_end})")
    except Exception as e:
        log.warning(f"Could not trigger resume: {e}")

# ── state ─────────────────────────────────────────────────────────────────────
_seen_lock = threading.Lock()

def load_seen():
    if Path("seen_ids.json").exists():
        with open("seen_ids.json", encoding="utf-8") as f:
            return set(json.load(f))
    return set()

def save_seen(seen):
    with _seen_lock:
        with open("seen_ids.json", "w", encoding="utf-8") as f:
            json.dump(list(seen), f)

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

def classify_device(model_name, display_size_str):
    name = model_name.lower()
    if any(k in name for k in ["watch", "band", "gear s", "galaxy fit", "amazfit", "mi band"]):
        return "Watch"
    if any(k in name for k in ["tab ", "tablet", " pad", "mediapad", "matebook", "iconia"]):
        return "Tablet"
    try:
        size = float(display_size_str)
        if size < 2.0: return "Watch"
        if size >= 7.0: return "Tablet"
    except: pass
    return "Phone"

def extract_specs(soup):
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

def scrape_one(session, brand, title, url):
    html = fetch(session, url)
    if html == "BLOCKED": return None, True
    if not html: return None, False

    soup          = BeautifulSoup(html, "html.parser")
    h1            = soup.find("h1", class_="specs-phone-name-title")
    model_name    = h1.get_text(strip=True) if h1 else title
    note          = soup.find("p", attrs={"data-spec": "comment"})
    also_known_as = note.get_text(strip=True) if note else ""
    dynamic       = extract_specs(soup)

    dim_raw   = dynamic.get("Body_Dimensions", "")
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
    display_size_in        = parse_display_size(ds_raw)
    disp_w, disp_h, ppi   = parse_resolution(dr_raw)
    storage_opts, ram_opts = parse_memory(mem_raw)

    fixed = {
        "Brand":                brand,
        "Model":                model_name,
        "Also Known As":        also_known_as,
        "Device Type":          classify_device(model_name, display_size_in),
        "URL":                  url,
        "Scraped At":           datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "Announced":            announced,
        "Release Year":         parse_release_year(status, announced),
        "Status":               status,
        "Length mm":            dim_l,
        "Width mm":             dim_w,
        "Height mm":            dim_h,
        "Weight g":             parse_weight(dynamic.get("Body_Weight", "")),
        "Display Size inches":  display_size_in,
        "Resolution Width px":  disp_w,
        "Resolution Height px": disp_h,
        "PPI":                  ppi,
        "Storage Options":      storage_opts,
        "RAM Options":          ram_opts,
        "Main Camera MP":       parse_camera_mp(c1_raw),
        "Selfie Camera MP":     parse_camera_mp(c2_raw),
        "Battery mAh":          parse_battery(bat_raw),
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
    return row, False

def save_outputs(results, base_name="gsmarena_devices"):
    if not results: return
    df   = pd.DataFrame(results)
    fp   = [c for c in FIXED_COLS   if c in df.columns]
    dp   = [c for c in DERIVED_COLS if c in df.columns and c not in fp]
    rest = sorted([c for c in df.columns if c not in fp and c not in dp])
    df   = df[fp + dp + rest]

    # save dated new-devices file — only this batch
    date_str  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    new_csv   = f"gsmarena_new_{date_str}.csv"
    new_xlsx  = f"gsmarena_new_{date_str}.xlsx"

    # if dated file already exists today, append to it
    if Path(new_csv).exists():
        try:
            today = pd.read_csv(new_csv, encoding="utf-8-sig")
            day_df = pd.concat([today, df], ignore_index=True)
            day_df = day_df.drop_duplicates(subset=["URL"], keep="last")
        except:
            day_df = df
    else:
        day_df = df

    day_df.to_csv(new_csv, index=False, encoding="utf-8-sig")
    try:
        day_df.to_excel(new_xlsx, index=False, engine="openpyxl")
    except: pass
    log.info(f"New devices file: {new_csv} ({len(day_df)} rows)")

    # merge into master file
    csv_path = f"{base_name}.csv"
    if Path(csv_path).exists():
        try:
            existing = pd.read_csv(csv_path, encoding="utf-8-sig")
            df = pd.concat([existing, df], ignore_index=True)
            df = df.drop_duplicates(subset=["URL"], keep="last")
        except: pass

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    try:
        df.to_excel(f"{base_name}.xlsx", index=False, engine="openpyxl")
    except: pass
    log.info(f"Master file: {len(df)} total rows")


def main():
    # Read chunk config from environment
    chunk_id    = int(os.environ.get("CHUNK_ID", "0"))
    chunk_start = int(os.environ.get("CHUNK_START", "0"))
    chunk_end   = int(os.environ.get("CHUNK_END", "0"))

    log.info(f"=== Chunk {chunk_id} | devices {chunk_start}-{chunk_end} ===")

    # Load seen
    seen = load_seen()

    # Load missing_devices.json — pre-filtered list from launcher
    # This contains ONLY devices not yet scraped
    with open("missing_devices.json", encoding="utf-8") as f:
        all_devices = json.load(f)

    log.info(f"missing_devices.json has {len(all_devices)} devices")

    # Get this chunk's slice
    my_devices = all_devices[chunk_start:chunk_end]
    # Final safety check against seen_ids
    todo = [d for d in my_devices if d["url"] not in seen]

    log.info(f"Chunk {chunk_id}: {len(my_devices)} assigned | {len(todo)} remaining")

    if not todo:
        log.info(f"Chunk {chunk_id} already complete!")
        return

    session  = make_session()
    results  = []
    seen_new = set()

    for i, device in enumerate(todo, 1):
        row, blocked = scrape_one(session, device["brand"], device["title"], device["url"])

        if blocked:
            log.warning(f"Chunk {chunk_id} BLOCKED at device {i}/{len(todo)} — triggering resume with fresh IP")
            # save what we have
            if results:
                save_outputs(results)
                seen.update(seen_new)
                save_seen(seen)
            # trigger new job for this same chunk
            trigger_resume(chunk_id, chunk_start, chunk_end)
            os._exit(2)

        if row:
            results.append(row)
            seen_new.add(device["url"])
            log.info(f"[Chunk {chunk_id}] [{i}/{len(todo)}] OK  {device['brand']} - {device['title']}")
        else:
            log.warning(f"[Chunk {chunk_id}] [{i}/{len(todo)}] FAIL {device['url']}")

        # checkpoint every 50
        if i % 50 == 0:
            save_outputs(results)
            seen.update(seen_new)
            save_seen(seen)
            results  = []
            seen_new = set()
            log.info(f"Chunk {chunk_id} checkpoint at {i}")

    # final save
    if results:
        save_outputs(results)
        seen.update(seen_new)
        save_seen(seen)

    log.info(f"=== Chunk {chunk_id} DONE ===")


if __name__ == "__main__":
    main()
