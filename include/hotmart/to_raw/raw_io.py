"""Zona RAW do hotmart — promoção stage→raw + leitura da partição do dia.

stage_to_raw(src): promove os JSON do stage p/ a RAW (append-only, ingestion_date)
e limpa o stage (só arquivos; pastas permanentes ficam).
read_raw_latest(src): lê a partição ingestion_date do dia (a recém-promovida).
"""
import json
from datetime import date

import polars as pl

from hotmart_config import get_container_client
from hotmart_meta import STAGE_DIR


def ingestion_date():
    return date.today().isoformat()


def stage_to_raw(src, ing_date=None):
    ing = ing_date or ingestion_date()
    prefix = f"{STAGE_DIR[src]}/"
    stage = get_container_client("stage")
    raw = get_container_client("raw")
    files = [
        b for b in stage.list_blobs(name_starts_with=prefix, include=["metadata"])
        if b.name.endswith(".json") and (b.metadata or {}).get("hdi_isfolder") != "true"
    ]
    for b in files:
        data = stage.download_blob(b.name).readall()
        fname = b.name.rsplit("/", 1)[-1]
        raw.upload_blob(name=f"{STAGE_DIR[src]}/ingestion_date={ing}/{fname}", data=data, overwrite=True)
    for b in files:
        stage.delete_blob(b.name)
    print(f"⬆️ stage→raw {src}: {len(files)} arquivo(s) promovidos; stage limpa (pastas preservadas)")


def read_raw_latest(src, ing_date=None):
    ing = ing_date or ingestion_date()
    c = get_container_client("raw")
    part_prefix = f"{STAGE_DIR[src]}/ingestion_date={ing}/"
    recs = []
    for b in c.list_blobs(name_starts_with=part_prefix):
        if not b.name.endswith(".json"):
            continue
        recs.extend(json.loads(c.download_blob(b.name).readall()))
    if not recs:
        print(f"📭 raw vazio: {part_prefix} (sem dados nesta janela)")
        return pl.DataFrame()
    print(f"📤 raw lido: {part_prefix} ({len(recs)} registros)")
    return pl.DataFrame(recs)
