"""Zona RAW do facebook — I/O (JSON append-only, particionado por ingestion_date).

write_raw: grava o JSON cru de um dia. read_raw_latest: lê a partição
ingestion_date mais recente (a recém-gravada pela ingestão). Caminhos e nomes
de arquivo vêm de fb_meta.ENTITIES.
"""
import json
from datetime import date

import polars as pl
from azure.storage.blob import ContentSettings

from facebook_config import RAW, get_blob_service_client
from fb_meta import ENTITIES


def ingestion_date():
    return date.today().isoformat()


def write_raw(entity, data_list, event_date, ing_date=None):
    """Grava o JSON cru de UM dia na raw: <raw_dir>/ingestion_date=<X>/<stem>_<dia>.json."""
    cfg = ENTITIES[entity]
    ing_date = ing_date or ingestion_date()
    path = f"{cfg['raw_dir']}/ingestion_date={ing_date}/{cfg['stem']}_{event_date}.json"
    blob = get_blob_service_client().get_blob_client(container=RAW, blob=path)
    blob.upload_blob(
        json.dumps(data_list, ensure_ascii=False, indent=2).encode("utf-8"),
        overwrite=True,
        content_settings=ContentSettings(content_type="application/json"),
    )
    print(f"📥 raw: {RAW}/{path} ({len(data_list)} registros)")


def read_raw_latest(entity):
    """DataFrame da partição ingestion_date MAIS RECENTE da raw (a recém-gravada)."""
    cfg = ENTITIES[entity]
    c = get_blob_service_client().get_container_client(RAW)
    prefix = f"{cfg['raw_dir']}/"
    parts = set()
    for b in c.list_blobs(name_starts_with=prefix):
        seg = b.name[len(prefix):].split("/")[0]
        if seg.startswith("ingestion_date="):
            parts.add(seg)
    if not parts:
        return pl.DataFrame()
    latest = max(parts)
    recs = []
    for b in c.list_blobs(name_starts_with=f"{prefix}{latest}/"):
        if not b.name.endswith(".json"):
            continue
        recs.extend(json.loads(c.download_blob(b.name).readall()))
    print(f"📤 raw lido: {cfg['raw_dir']}/{latest} ({len(recs)} registros)")
    return pl.DataFrame(recs) if recs else pl.DataFrame()
