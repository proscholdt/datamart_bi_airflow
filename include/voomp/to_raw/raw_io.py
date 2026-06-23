"""Zona RAW do voomp — I/O (cópia crua do Excel, append-only por ingestion_date)."""
import io
from datetime import date

import polars as pl

from voomp_config import get_container_client


def ingestion_date():
    return date.today().isoformat()


def read_any(buf, name):
    """Lê xlsx/xls (read_excel) ou csv (read_csv) de um buffer."""
    return pl.read_csv(buf) if name.lower().endswith(".csv") else pl.read_excel(buf)


def write_raw(raw_prefix, filename, data_bytes, ing_date=None):
    """Grava bytes crus na RAW: <raw_prefix>/ingestion_date=<X>/<filename>."""
    ing_date = ing_date or ingestion_date()
    raw_name = f"{raw_prefix}/ingestion_date={ing_date}/{filename}"
    get_container_client("raw").upload_blob(name=raw_name, data=data_bytes, overwrite=True)
    print(f"📥 raw: {raw_name}")
    return raw_name


def latest_raw_file(raw_prefix, exts):
    """(blob_name) do arquivo na partição ingestion_date mais recente da RAW; None se vazio."""
    c = get_container_client("raw")
    parts = {}
    for b in c.list_blobs(name_starts_with=raw_prefix + "/"):
        rel = b.name[len(raw_prefix) + 1:]
        seg = rel.split("/")[0]
        if seg.startswith("ingestion_date=") and b.name.lower().endswith(exts):
            parts.setdefault(seg, []).append(b.name)
    if not parts:
        return None
    return sorted(parts[max(parts)])[0]


def download_raw(blob_name):
    """BytesIO de um blob da RAW."""
    buf = io.BytesIO()
    get_container_client("raw").download_blob(blob_name).readinto(buf)
    buf.seek(0)
    return buf
