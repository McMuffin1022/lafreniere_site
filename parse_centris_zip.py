#!/usr/bin/env python3
"""
Fetch latest NOMADESMARKETINGYYYYMMDD.zip from a Centris folder URL,
then parse it to a normalized JSON similar to the expected schema.

Usage:
  python fetch_and_parse_centris.py \
    --base-url https://lpsep9.n0c.world/centris/ \
    --output-dir /var/centris_out \
    --retries 18 \
    --retry-seconds 300

Defaults pull the index page to discover the newest date. If the index
is not accessible, the script tries today's and yesterday's ZIP names.
"""

import argparse
import sys, re, os, io, json, time, math
import datetime as dt
from zoneinfo import ZoneInfo
from typing import List, Optional, Tuple
from collections import defaultdict, OrderedDict
import zipfile, csv, requests

DEFAULT_BASE_URL = "https://lpsep9.n0c.world/centris/"
UA = "SebasIT-CentrisFetcher/1.0 (+https://example.local)"
TZ = ZoneInfo("America/Toronto")

# ---------------- Parsing helpers (from parse_centris_zip.py, inlined) ---------------- #
CAT_LABEL = {
    'ALLE': 'Allée',
    'CHAU': 'Mode de chauffage',
    'EAU': 'Approvisionnement en eau',
    'ENER': 'Énergie pour chauffage',
    'FENE': 'Fenestration',
    'FOND': 'Fondation',
    'PARE': 'Revêtement extérieur',
    'SS': 'Sous-sol',
    'SYEG': 'Système d\'égout',
    'TFEN': 'Type de fenestration',
    'VUE': 'Vue',
    'ZONG': 'Zonage',
    'PROX': 'Proximité',
}

VAL_LABEL = {
    'NPAV': 'Non pavé',
    'PELC': 'Plinthes électriques',
    'AMU': 'Municipal',
    'ELEC': 'Électricité',
    'BOIS': 'BOIS',
    'PVC': 'PVC',
    'BETO': 'Béton',
    'AU': 'Autre',
    'VSAN': 'Vide sanitaire',
    'EGMU': 'Égout municipal',
    'COUL': 'COUL',
    'PFEN': 'PFEN',
    'EAU': "Vue sur l'eau",
    'RES': 'Résidentiel',
    'AUTO': 'Autoroute',
    'PCYC': 'Piste cyclable',
    'PRIM': 'École primaire',
    'SEC': 'École secondaire',
    'TRSP': 'Transport en commun',
}

def clean(s):
    return s.strip().strip('"') if isinstance(s, str) else s

def read_csv_from_zip(z: zipfile.ZipFile, name: str, encoding='cp1252'):
    if name not in z.namelist(): return []
    txt = z.read(name).decode(encoding, errors='replace')
    return list(csv.reader(io.StringIO(txt)))

def extract_year(values):
    for v in values:
        vs = clean(v)
        if vs and vs.isdigit() and len(vs) == 4:
            y = int(vs)
            if 1800 <= y <= 2035:
                return y
    return None

def extract_address(row):
    civic = clean(row[25]) if len(row) > 25 else ''
    street = clean(row[27]) if len(row) > 27 else ''
    postal = clean(row[29]) if len(row) > 29 else ''
    parts = [p for p in [civic, street, postal] if p]
    return ", ".join(parts) if parts else None

def extract_price(row):
    if len(row) > 6 and clean(row[6]).isdigit():
        return int(clean(row[6]))
    return None

def extract_description(remarques_rows):
    # rows like: id, seq (1..), "F", ..., ..., ..., text
    chosen = [r for r in remarques_rows if len(r) >= 7 and clean(r[2]) == 'F']
    try:
        chosen.sort(key=lambda r: int(clean(r[1]) or 0))
    except Exception:
        pass
    text = " ".join(clean(r[-1]) for r in chosen if clean(r[-1]))
    text = re.sub(r'<br\\s*/?>', ' ', text, flags=re.I)
    return text.strip() or None

def extract_proximites(addenda_text, carac_rows):
    prox = []
    for r in carac_rows:
        if len(r) >= 3 and clean(r[1]) == 'PROX':
            prox.append(VAL_LABEL.get(clean(r[2]), clean(r[2])))
    if (not prox) and addenda_text:
        m = re.search(r'À\\s*proximité\\s*:?\\s*(.+)$', addenda_text, flags=re.I|re.S)
        if m:
            seg = m.group(1)
            for item in re.split(r'[;,]\\s*', seg):
                item = re.sub(r'<.*?>', '', item).strip()
                if item: prox.append(item)
    seen, out = set(), []
    for p in prox:
        if p and p not in seen:
            seen.add(p); out.append(p)
    return ", ".join(out) if out else None

def extract_photos(photo_rows):
    urls = []
    for r in photo_rows:
        if len(r) >= 7 and str(r[6]).startswith('http'):
            urls.append(clean(r[6]))
    return ", ".join(urls) if urls else None

def extract_units(units_rows, pieces_rows):
    principal = None
    try:
        for r in sorted(units_rows, key=lambda r: int(clean(r[1]) or 999)):
            if clean(r[1]) == '1':
                principal = r; break
    except Exception:
        if units_rows:
            principal = units_rows[0]
    pieces = chambres = sdb = None
    if principal and len(principal) >= 5:
        p = clean(principal[3])
        c = clean(principal[4])
        pieces = int(p) if p and p.isdigit() else None
        chambres = int(c) if c and c.isdigit() else None
    sdb = sum(1 for r in pieces_rows if len(r) >= 4 and clean(r[1])=='1' and clean(r[3])=='SDB') or None
    return pieces, chambres, sdb

def extract_addenda(z, id_):
    if 'ADDENDA.TXT' not in z.namelist(): return None
    txt = z.read('ADDENDA.TXT').decode('cp1252', errors='replace')
    reader = csv.reader(io.StringIO(txt))
    chunks = [clean(r[-1]) for r in reader if r and clean(r[0])==id_ and r[-1]]
    if chunks:
        joined = " ".join(chunks)
        joined = re.sub(r'<br\\s*/?>', ' ', joined, flags=re.I)
        return joined
    return None

def parse_zipfile_to_json(zip_path: str):
    with zipfile.ZipFile(zip_path, 'r') as z:
        rows_ins = read_csv_from_zip(z, 'INSCRIPTIONS.TXT')
        rows_rem = read_csv_from_zip(z, 'REMARQUES.TXT')
        rows_car = read_csv_from_zip(z, 'CARACTERISTIQUES.TXT')
        rows_pho = read_csv_from_zip(z, 'PHOTOS.TXT')
        rows_uni = read_csv_from_zip(z, 'UNITES_DETAILLEES.TXT')
        rows_pie = read_csv_from_zip(z, 'PIECES_UNITES.TXT')

        by_rem = defaultdict(list); [by_rem[clean(r[0])].append(r) for r in rows_rem if r]
        by_car = defaultdict(list); [by_car[clean(r[0])].append(r) for r in rows_car if r]
        by_pho = defaultdict(list); [by_pho[clean(r[0])].append(r) for r in rows_pho if r]
        by_uni = defaultdict(list); [by_uni[clean(r[0])].append(r) for r in rows_uni if r]
        by_pie = defaultdict(list); [by_pie[clean(r[0])].append(r) for r in rows_pie if r]

        listings = []
        for row in rows_ins:
            if not row or not clean(row[0]): continue
            id_ = clean(row[0])
            rec = OrderedDict()
            rec["id"] = id_
            rec["slug"] = f"listing-{id_}"
            # Price, address
            rec["prix"] = extract_price(row)
            rec["adresse"] = extract_address(row)
            # Units
            pieces, chambres, sdb = extract_units(by_uni.get(id_, []), by_pie.get(id_, []))
            rec["nombre_pieces"] = pieces
            rec["nombre_chambres"] = chambres
            rec["nombre_sdb"] = sdb
            # Optional sizes (left None unless mapped)
            rec["superficie_habitable"] = None
            rec["superficie_terrain"] = None
            # Year
            rec["annee_construction"] = extract_year(row)
            rec["inclus"] = None
            # Texts
            rec["description"] = extract_description(by_rem.get(id_, []))
            rec["proximites"] = extract_proximites(extract_addenda(z, id_) or '', by_car.get(id_, []))
            rec["photos"] = extract_photos(by_pho.get(id_, []))
            # Caracteristiques
            car_descs = []
            for r in by_car.get(id_, []):
                cat = clean(r[1]); val = clean(r[2]); label = VAL_LABEL.get(val, val)
                if cat == 'PROX': continue
                car_label = CAT_LABEL.get(cat, cat)
                extra = clean(r[3]) if len(r) > 3 else ''
                suffix = f" ({extra})" if extra else ''
                car_descs.append(f"{car_label}: {label}{suffix}")
            rec["caracteristiques"] = ", ".join(car_descs) if car_descs else None
            listings.append(rec)
        return listings

# ---------------- Fetch helpers ---------------- #
def list_zip_dates_from_index(session: requests.Session, base_url: str) -> List[dt.date]:
    """Parse the folder index HTML for NOMADESMARKETINGYYYYMMDD.zip and return sorted dates (asc)."""
    r = session.get(base_url, timeout=20)
    r.raise_for_status()
    # find filenames in HTML
    dates = set()
    for m in re.finditer(r'NOMADESMARKETING(\d{8})\.zip', r.text):
        ymd = m.group(1)
        try:
            d = dt.datetime.strptime(ymd, "%Y%m%d").date()
            dates.add(d)
        except ValueError:
            pass
    return sorted(dates)

def compose_url_for_date(base_url: str, d: dt.date) -> str:
    return base_url.rstrip('/') + f"/NOMADESMARKETING{d.strftime('%Y%m%d')}.zip"

def try_head(session: requests.Session, url: str) -> Optional[int]:
    try:
        h = session.head(url, timeout=15, allow_redirects=True)
        if h.status_code == 200:
            size = int(h.headers.get("Content-Length", "0")) if h.headers.get("Content-Length") else None
            return size or 1
        return None
    except requests.RequestException:
        return None

def download(session: requests.Session, url: str, dest_path: str) -> int:
    with session.get(url, timeout=60, stream=True) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1<<16):
                if chunk:
                    f.write(chunk)
    return os.path.getsize(dest_path)

def find_latest_available(session: requests.Session, base_url: str, today: dt.date) -> Tuple[str, dt.date]:
    # Prefer index listing
    try:
        dates = list_zip_dates_from_index(session, base_url)
        if dates:
            best = max(d for d in dates if d <= today) if any(d <= today for d in dates) else max(dates)
            return compose_url_for_date(base_url, best), best
    except Exception:
        pass
    # Fallback: try today, yesterday, and -2 days
    for delta in range(0, 3):
        d = today - dt.timedelta(days=delta)
        url = compose_url_for_date(base_url, d)
        if try_head(session, url):
            return url, d
    raise RuntimeError("No recent NOMADESMARKETINGYYYYMMDD.zip found (today..today-2).")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Folder URL ending with /centris/")
    ap.add_argument("--output-dir", default="./out", help="Directory to save zips and JSON")
    ap.add_argument("--retries", type=int, default=1, help="Number of retries if the file is not yet available")
    ap.add_argument("--retry-seconds", type=int, default=0, help="Seconds to wait between retries")
    args = ap.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    session = requests.Session()
    session.headers.update({"User-Agent": UA})

    attempt = 0
    last_err = None
    while attempt < max(1, args.retries):
        attempt += 1
        now = dt.datetime.now(TZ)
        today = now.date()
        try:
            url, d = find_latest_available(session, args.base_url, today)
            fname = f"NOMADESMARKETING{d.strftime('%Y%m%d')}.zip"
            zip_path = os.path.join(args.output_dir, fname)
            print(f"[{now.isoformat()}] Latest = {fname} -> {url}")
            size = try_head(session, url) or 0
            print(f"[info] Remote size ~ {size} bytes (if provided)")
            downloaded = download(session, url, zip_path)
            print(f"[ok] Downloaded {downloaded} bytes to {zip_path}")
            # Parse
            listings = parse_zipfile_to_json(zip_path)
            out_json = os.path.join(args.output_dir, f"{fname[:-4]}.json")
            with open(out_json, "w", encoding="utf-8") as f:
                json.dump(listings, f, ensure_ascii=False, indent=2)
            # Write/refresh a 'latest.json' symlink/copy
            latest_json = os.path.join(args.output_dir, "latest.json")
            try:
                if os.path.islink(latest_json) or os.path.exists(latest_json):
                    os.remove(latest_json)
                # Symlink if possible, else copy
                try:
                    os.symlink(out_json, latest_json)
                except Exception:
                    import shutil
                    shutil.copyfile(out_json, latest_json)
            except Exception as e:
                print(f"[warn] Could not update latest.json: {e}", file=sys.stderr)
            print(f"[done] Parsed {len(listings)} listings -> {out_json} (and latest.json)")
            return 0
        except Exception as e:
            last_err = e
            print(f"[attempt {attempt}] Not ready or failed: {e}", file=sys.stderr)
            if attempt < max(1, args.retries):
                time.sleep(max(0, args.retry_seconds))
    print(f"[error] Failed after {attempt} attempt(s): {last_err}", file=sys.stderr)
    return 2

if __name__ == "__main__":
    sys.exit(main())
