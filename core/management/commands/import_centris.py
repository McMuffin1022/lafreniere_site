# core/management/commands/import_centris.py
import csv
import io
import json
import re
import time
import zipfile
from datetime import datetime, timedelta, date
from io import BytesIO
from typing import List, Optional, Tuple
from collections import defaultdict, OrderedDict

import requests
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone

from core.models import Listing, ListingPhoto, FetchLog

# -------------------- MAPPINGS -------------------- #
CAT_LABEL = {
    'ALLE': 'Allée',
    'CHAU': 'Mode de chauffage',
    'EAU': 'Approvisionnement en eau',
    'ENER': 'Énergie pour chauffage',
    'FENE': 'Fenestration',
    'FOND': 'Fondation',
    'PARE': 'Revêtement extérieur',
    'SS': 'Sous-sol',
    'SYEG': "Système d'égout",
    'TFEN': 'Type de fenestration',
    'VUE': 'Vue',
    'ZONG': 'Zonage',
    'PROX': 'Proximité',
}

VAL_LABEL = {
    # ALLE
    'NPAV': 'Non pavé',
    # CHAU
    'PELC': 'Plinthes électriques',
    # EAU
    'AMU': 'Municipal',
    # ENER
    'ELEC': 'Électricité',
    # FENE
    'BOIS': 'BOIS',
    'PVC': 'PVC',
    # FOND
    'BETO': 'Béton',
    # PARE
    'AU': 'Autre',
    # SS
    'VSAN': 'Vide sanitaire',
    # SYEG
    'EGMU': 'Égout municipal',
    # TFEN
    'COUL': 'COUL',
    'PFEN': 'PFEN',
    # VUE
    'EAU': "Vue sur l'eau",
    # ZONG
    'RES': 'Résidentiel',
    # PROX
    'AUTO': 'Autoroute',
    'PCYC': 'Piste cyclable',
    'PRIM': 'École primaire',
    'SEC': 'École secondaire',
    'TRSP': 'Transport en commun',
}

UA = "SebasIT-CentrisImporter/1.0"
ENC = "cp1252"


# -------------------- HELPERS (parse) -------------------- #
def clean(s):
    return s.strip().strip('"') if isinstance(s, str) else s


def read_csv_from_zip(z: zipfile.ZipFile, name: str, encoding: str = ENC):
    if name not in z.namelist():
        return []
    txt = z.read(name).decode(encoding, errors='replace')
    return list(csv.reader(io.StringIO(txt)))


def extract_year(values) -> Optional[int]:
    for v in values:
        vs = clean(v)
        if vs and vs.isdigit() and len(vs) == 4:
            y = int(vs)
            if 1800 <= y <= 2035:
                return y
    return None


def extract_address(row) -> Optional[str]:
    # Heuristique validée sur tes fichiers:
    # [25] no civique, [27] rue, [29] code postal
    civic = clean(row[25]) if len(row) > 25 else ''
    street = clean(row[27]) if len(row) > 27 else ''
    postal = clean(row[29]) if len(row) > 29 else ''
    parts = [p for p in (civic, street, postal) if p]
    return ", ".join(parts) if parts else None


def extract_price(row) -> Optional[int]:
    if len(row) > 6 and clean(row[6]).isdigit():
        return int(clean(row[6]))
    return None


def extract_description(remarques_rows) -> Optional[str]:
    # rows: id, seq (1..), "F", ..., ..., ..., text
    chosen = [r for r in remarques_rows if len(r) >= 7 and clean(r[2]) == 'F']
    try:
        chosen.sort(key=lambda r: int(clean(r[1]) or 0))
    except Exception:
        pass
    text = " ".join(clean(r[-1]) for r in chosen if clean(r[-1]))
    text = re.sub(r'<br\s*/?>', ' ', text, flags=re.I)
    return text.strip() or None


def extract_addenda(z: zipfile.ZipFile, id_: str) -> Optional[str]:
    if 'ADDENDA.TXT' not in z.namelist():
        return None
    txt = z.read('ADDENDA.TXT').decode(ENC, errors='replace')
    reader = csv.reader(io.StringIO(txt))
    chunks = [clean(r[-1]) for r in reader if r and clean(r[0]) == id_ and r[-1]]
    if chunks:
        joined = " ".join(chunks)
        joined = re.sub(r'<br\s*/?>', ' ', joined, flags=re.I)
        return joined
    return None


def extract_proximites(addenda_text: str, carac_rows) -> Tuple[str, list]:
    prox = []
    for r in carac_rows:
        if len(r) >= 3 and clean(r[1]) == 'PROX':
            prox.append(VAL_LABEL.get(clean(r[2]), clean(r[2])))
    if (not prox) and addenda_text:
        m = re.search(r'À\s*proximité\s*:?\s*(.+)$', addenda_text, flags=re.I | re.S)
        if m:
            seg = m.group(1)
            for item in re.split(r'[;,]\s*', seg):
                item = re.sub(r'<.*?>', '', item).strip()
                if item:
                    prox.append(item)
    # unique, preserve order
    seen, out = set(), []
    for p in prox:
        if p and p not in seen:
            seen.add(p)
            out.append(p)
    return (", ".join(out) if out else ""), out


def extract_photos(photo_rows) -> List[Tuple[int, str]]:
    # columns (observé): id, seq, _, room_code, _, _, url, media_id, timestamp
    # On retourne [(sequence, url), ...]
    res = []
    for r in photo_rows:
        if len(r) >= 7:
            seq = clean(r[1])
            url = clean(r[6])
            if url and url.startswith("http"):
                try:
                    seq_i = int(seq) if (seq and seq.isdigit()) else 0
                except Exception:
                    seq_i = 0
                res.append((seq_i, url))
    res.sort(key=lambda t: t[0])
    return res


def extract_units(units_rows, pieces_rows) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    principal = None
    try:
        for r in sorted(units_rows, key=lambda r: int(clean(r[1]) or 999)):
            if clean(r[1]) == '1':
                principal = r
                break
    except Exception:
        if units_rows:
            principal = units_rows[0]
    pieces = chambres = None
    if principal and len(principal) >= 5:
        p = clean(principal[3]); c = clean(principal[4])
        pieces = int(p) if (p and p.isdigit()) else None
        chambres = int(c) if (c and c.isdigit()) else None
    sdb = sum(1 for r in pieces_rows if len(r) >= 4 and clean(r[1]) == '1' and clean(r[3]) == 'SDB') or None
    return pieces, chambres, sdb


def build_caracteristiques(carac_rows) -> Tuple[str, list]:
    descs = []
    arr = []
    for r in carac_rows:
        if len(r) < 3:
            continue
        cat = clean(r[1]); val = clean(r[2])
        if cat == 'PROX':
            continue
        car_label = CAT_LABEL.get(cat, cat)
        label = VAL_LABEL.get(val, val)
        extra = clean(r[3]) if len(r) > 3 else ''
        suffix = f" ({extra})" if extra else ''
        piece = f"{car_label}: {label}{suffix}"
        descs.append(piece)
        arr.append({"cat": car_label, "val": label})
    return (", ".join(descs) if descs else ""), arr


# -------------------- HELPERS (fetch) -------------------- #
def list_zip_dates_from_index(session: requests.Session, base_url: str) -> List[date]:
    r = session.get(base_url, timeout=20)
    r.raise_for_status()
    dates = set()
    for m in re.finditer(r'NOMADESMARKETING(\d{8})\.zip', r.text):
        ymd = m.group(1)
        try:
            d = datetime.strptime(ymd, "%Y%m%d").date()
            dates.add(d)
        except ValueError:
            pass
    return sorted(dates)


def compose_url_for_date(base_url: str, d: date) -> str:
    return base_url.rstrip('/') + f"/NOMADESMARKETING{d.strftime('%Y%m%d')}.zip"


def try_head(session: requests.Session, url: str) -> bool:
    try:
        h = session.head(url, timeout=15, allow_redirects=True)
        return h.status_code == 200
    except requests.RequestException:
        return False


def find_latest_available(session: requests.Session, base_url: str, today: date) -> Tuple[str, date]:
    # 1) essai via index
    try:
        dates = list_zip_dates_from_index(session, base_url)
        if dates:
            best = max(d for d in dates if d <= today) if any(d <= today for d in dates) else max(dates)
            return compose_url_for_date(base_url, best), best
    except Exception:
        pass
    # 2) fallback: today, -1, -2
    for delta in range(0, 3):
        d = today - timedelta(days=delta)
        url = compose_url_for_date(base_url, d)
        if try_head(session, url):
            return url, d
    raise RuntimeError("Aucun ZIP récent trouvé (aujourd'hui..avant-hier).")


def download_bytes(session: requests.Session, url: str) -> bytes:
    with session.get(url, timeout=60, stream=True) as r:
        r.raise_for_status()
        buf = BytesIO()
        for chunk in r.iter_content(chunk_size=1 << 16):
            if chunk:
                buf.write(chunk)
        return buf.getvalue()


# -------------------- DJANGO COMMAND -------------------- #
class Command(BaseCommand):
    help = "Fetch + parse + import Centris en un seul run (et marque SOLD ce qui disparaît)."

    def add_arguments(self, parser):
        parser.add_argument("--base-url", default="https://lpsep9.n0c.world/centris/", help="URL du dossier Centris (se termine par /centris/)")
        parser.add_argument("--retries", type=int, default=12, help="Nb de tentatives si le ZIP n'est pas prêt")
        parser.add_argument("--retry-seconds", type=int, default=300, help="Pause entre tentatives (sec)")
        parser.add_argument("--save-zip-dir", default="", help="Optionnel: dossier où sauvegarder le ZIP téléchargé")
        parser.add_argument("--no-mark-sold", action="store_true", help="Ne pas marquer SOLD les ID absents")

    def handle(self, *args, **opts):
        base_url = opts["base_url"].strip()
        retries = max(1, int(opts["retries"]))
        retry_seconds = max(0, int(opts["retry_seconds"]))
        save_zip_dir = opts["save_zip_dir"].strip()
        do_mark_sold = not opts["no_mark_sold"]

        session = requests.Session()
        session.headers.update({"User-Agent": UA})

        start_ts = time.monotonic()
        now = timezone.now()
        today = now.date()

        last_err = None
        source_name = ""
        file_date: Optional[date] = None
        data_bytes: Optional[bytes] = None

        # Fetch loop
        for attempt in range(1, retries + 1):
            try:
                url, d = find_latest_available(session, base_url, today)
                file_date = d
                source_name = url.rsplit("/", 1)[-1]
                self.stdout.write(f"[try {attempt}/{retries}] latest={source_name} -> {url}")
                data_bytes = download_bytes(session, url)
                self.stdout.write(self.style.SUCCESS(f"OK download ({len(data_bytes)} bytes)"))
                if save_zip_dir:
                    import os
                    os.makedirs(save_zip_dir, exist_ok=True)
                    with open(f"{save_zip_dir.rstrip('/')}/{source_name}", "wb") as f:
                        f.write(data_bytes)
                break
            except Exception as e:
                last_err = e
                self.stderr.write(f"[try {attempt}] failed: {e}")
                if attempt < retries:
                    time.sleep(retry_seconds)

        if data_bytes is None:
            raise CommandError(f"Echec de téléchargement après {retries} tentatives: {last_err}")

        # Parse + import
        added = updated = marked_sold = 0
        items_total = 0

        with zipfile.ZipFile(BytesIO(data_bytes), 'r') as z:
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

            seen_ids = set()

            with transaction.atomic():
                for row in rows_ins:
                    if not row or not clean(row[0]):
                        continue
                    items_total += 1
                    id_ = clean(row[0])
                    seen_ids.add(id_)

                    prix = extract_price(row)
                    adresse = (extract_address(row) or "")
                    annee = extract_year(row)
                    descr = (extract_description(by_rem.get(id_, [])) or "")

                    # proximites
                    addenda_txt = extract_addenda(z, id_) or ""
                    proximites_text, proximites_arr = extract_proximites(addenda_txt, by_car.get(id_, []))

                    # caracteristiques
                    car_text, car_arr = build_caracteristiques(by_car.get(id_, []))

                    # units / rooms
                    n_pieces, n_chambres, n_sdb = extract_units(by_uni.get(id_, []), by_pie.get(id_, []))

                    obj, created = Listing.objects.get_or_create(
                        centris_id=id_,
                        defaults=dict(
                            slug=f"listing-{id_}",
                            prix=prix,
                            adresse=adresse,
                            nombre_pieces=n_pieces,
                            nombre_chambres=n_chambres,
                            nombre_sdb=n_sdb,
                            superficie_habitable=None,
                            superficie_terrain=None,
                            annee_construction=annee,
                            inclus="",  # pas mappé pour l’instant
                            description=descr,
                            proximites_text=proximites_text,
                            proximites=proximites_arr,
                            caracteristiques_text=car_text,
                            caracteristiques=car_arr,
                            status=Listing.STATUS_ACTIVE,
                            sold_at=None,
                            last_seen_at=now,
                        ),
                    )
                    if created:
                        obj.ensure_slug()
                        obj.save(update_fields=["slug"])
                        added += 1
                    else:
                        obj.prix = prix
                        obj.adresse = adresse
                        obj.nombre_pieces = n_pieces
                        obj.nombre_chambres = n_chambres
                        obj.nombre_sdb = n_sdb
                        obj.superficie_habitable = None
                        obj.superficie_terrain = None
                        obj.annee_construction = annee
                        obj.inclus = ""
                        obj.description = descr
                        obj.proximites_text = proximites_text
                        obj.proximites = proximites_arr
                        obj.caracteristiques_text = car_text
                        obj.caracteristiques = car_arr
                        obj.last_seen_at = now
                        obj.status = Listing.STATUS_ACTIVE
                        obj.sold_at = None
                        obj.ensure_slug()
                        obj.save()
                        updated += 1

                    # photos
                    photos = extract_photos(by_pho.get(id_, []))
                    if photos:
                        ListingPhoto.objects.filter(listing=obj).delete()
                        ListingPhoto.objects.bulk_create(
                            [ListingPhoto(listing=obj, sequence=(i + 1), url=u) for i, (_, u) in enumerate(photos)]
                        )

                # Mark SOLD for missing
                if do_mark_sold:
                    sold_qs = Listing.objects.filter(status=Listing.STATUS_ACTIVE).exclude(centris_id__in=seen_ids)
                    marked_sold = sold_qs.update(status=Listing.STATUS_SOLD, sold_at=now)

                # Log
                duration = time.monotonic() - start_ts
                FetchLog.objects.create(
                    file_date=file_date,
                    source_url=base_url,
                    source_name=source_name,
                    items_total=items_total,
                    items_added=added,
                    items_updated=updated,
                    items_marked_sold=marked_sold,
                    duration_seconds=duration,
                )

        self.stdout.write(self.style.SUCCESS(
            f"Import Centris OK: total={items_total} +{added} ~{updated} sold={marked_sold}"
        ))
