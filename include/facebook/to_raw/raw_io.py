"""Zona RAW do facebook — promoção stage→raw + leitura da raw.

stage_to_raw: lê os JSON que o ingest gravou no STAGE, promove p/ a RAW
(append-only, particionada por ingestion_date) e LIMPA o stage — apaga só os
ARQUIVOS, preservando as pastas permanentes (hdi_isfolder).
read_raw_latest: lê a partição ingestion_date mais recente p/ o bronze.
"""
import json
from datetime import date

import polars as pl

from facebook_config import get_container_client
from fb_meta import ENTITIES


def ingestion_date():
    return date.today().isoformat()


def stage_to_raw(entity, ing_date=None):
    """Promove os JSON do STAGE p/ a RAW (ingestion_date) e limpa o stage (só arquivos)."""
    cfg = ENTITIES[entity]
    ing = ing_date or ingestion_date()
    prefix = f"{cfg['raw_dir']}/"  # source_facebook/facebook_<entity>/
    stage = get_container_client("stage")
    raw = get_container_client("raw")

    files = [
        b for b in stage.list_blobs(name_starts_with=prefix, include=["metadata"])
        if b.name.endswith(".json") and (b.metadata or {}).get("hdi_isfolder") != "true"
    ]
    for b in files:
        data = stage.download_blob(b.name).readall()
        fname = b.name.rsplit("/", 1)[-1]
        raw.upload_blob(name=f"{cfg['raw_dir']}/ingestion_date={ing}/{fname}", data=data, overwrite=True)
    # limpa o stage: apaga SÓ os arquivos; as pastas (hdi_isfolder) ficam permanentes.
    for b in files:
        stage.delete_blob(b.name)
    print(f"⬆️ stage→raw {entity}: {len(files)} arquivo(s) promovidos; stage limpa (pastas preservadas)")


def read_raw_latest(entity, ing_date=None):
    """DataFrame da partição ingestion_date DO RUN (default = hoje, a recém-promovida).

    Lê SÓ `ingestion_date=<ing>/` (não enumera o histórico append-only da raw). Se o
    stage veio vazio nessa janela, não há partição de hoje → DataFrame vazio (o
    raw→bronze não muda nada), em vez de remesclar silenciosamente um dia anterior.
    """
    cfg = ENTITIES[entity]
    c = get_container_client("raw")
    ing = ing_date or ingestion_date()
    part_prefix = f"{cfg['raw_dir']}/ingestion_date={ing}/"
    recs = []
    for b in c.list_blobs(name_starts_with=part_prefix):
        if not b.name.endswith(".json"):
            continue
        recs.extend(json.loads(c.download_blob(b.name).readall()))
    if not recs:
        print(f"📭 raw vazio: {part_prefix} (sem dados nesta janela; bronze não muda)")
        return pl.DataFrame()
    print(f"📤 raw lido: {part_prefix} ({len(recs)} registros)")
    return pl.DataFrame(recs)
