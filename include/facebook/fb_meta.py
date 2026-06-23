"""Metadados das entidades do data mart facebook + I/O da zona raw.

Centraliza, por entidade (camp/adset/ad): o grão (chave do MERGE), as colunas
do fato e da dimensão, os caminhos das tabelas Delta (bronze/silver/gold) e a
zona raw (JSON append-only particionada por ingestion_date).

Os scripts de ingestão e os transforms (fb_transforms.py) importam daqui.
"""
import json
import os
from datetime import date, timedelta

import polars as pl
from azure.storage.blob import ContentSettings

from facebook_config import RAW, BRONZE, SILVER, GOLD, get_blob_service_client
from common.delta_io import delta_uri, read_watermark

# Janela de restatement da Meta (re-extrai/re-mescla os últimos N dias).
LOOKBACK_DAYS = 10
# 1ª carga (bronze vazio) sem FB_LOAD_START_DATE: quantos dias retroagir.
DEFAULT_BACKFILL_DAYS = 90

# Medidas (mesmo conjunto das 3 entidades). ctr/cpc/frequency/cost_per_unique_click
# são TAXAS (não somáveis) — o grão diário-por-entidade já é único, então o fato
# apenas deduplica (não soma) para preservar as taxas.
MEASURE_FLOAT = ["spend", "ctr", "cpc", "frequency", "cost_per_unique_click", "purchase_value"]
MEASURE_INT = ["impressions", "clicks", "reach", "leads", "purchases"]
MEASURES = MEASURE_FLOAT + MEASURE_INT

ENTITIES = {
    "camp": {
        "level": "campaign",
        "raw_dir": "source_facebook/facebook_camp",  # = DEST_FOLDER do ingest
        "stem": "campaigns",
        "id_col": "campaign_id",
        "grain": ["data", "campaign_id"],
        "fk_cols": [],                                # FKs além do próprio id
        "dim_name": "dim_camp",
        "dim_cols": ["campaign_id", "campaign_name"],
        "dim_key": ["campaign_id"],
    },
    "adset": {
        "level": "adset",
        "raw_dir": "source_facebook/facebook_adset",
        "stem": "adsets",
        "id_col": "adset_id",
        "grain": ["data", "adset_id"],
        "fk_cols": ["campaign_id"],
        "dim_name": "dim_adset",
        "dim_cols": ["adset_id", "adset_name", "publicos_personalizados_incluidos"],
        "dim_key": ["adset_id"],
    },
    "ad": {
        "level": "ad",
        "raw_dir": "source_facebook/facebook_ad",
        "stem": "ads",
        "id_col": "ad_id",
        "grain": ["data", "ad_id"],
        "fk_cols": ["adset_id", "campaign_id"],
        "dim_name": "dim_ad",
        "dim_cols": ["ad_id", "ad_name", "video_url", "video_url_click", "video_thumbnail_url"],
        "dim_key": ["ad_id"],
    },
}


# --------------------------------------------------------------------------- #
# URIs das tabelas Delta
# --------------------------------------------------------------------------- #
def bronze_uri(entity):
    return delta_uri(BRONZE, f"facebook/bronze_{entity}")


def silver_uri(entity):
    return delta_uri(SILVER, f"facebook/silver_{entity}")


def fact_uri(entity):
    return delta_uri(GOLD, f"facebook/f_{entity}")


def dim_uri(entity):
    return delta_uri(GOLD, f"facebook/{ENTITIES[entity]['dim_name']}")


def grain_predicate(entity):
    """Predicate do MERGE por grão: t.data = s.data AND t.<id> = s.<id>."""
    return " AND ".join(f"t.{c} = s.{c}" for c in ENTITIES[entity]["grain"])


def dim_predicate(entity):
    return " AND ".join(f"t.{c} = s.{c}" for c in ENTITIES[entity]["dim_key"])


# --------------------------------------------------------------------------- #
# Janela da API (ingestão): watermark do bronze - lookback, até D-1
# --------------------------------------------------------------------------- #
def api_window(entity):
    """(start, end) ISO p/ extração. Backfill por env, senão watermark+lookback."""
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    s_env, e_env = os.getenv("FB_LOAD_START_DATE"), os.getenv("FB_LOAD_END_DATE")
    if s_env and e_env:
        return s_env, e_env

    wm = read_watermark(bronze_uri(entity), "data")
    if wm is None:
        start = s_env or (date.today() - timedelta(days=1 + DEFAULT_BACKFILL_DAYS)).isoformat()
        return start, yesterday
    start = (wm - timedelta(days=LOOKBACK_DAYS)).isoformat()
    return start, yesterday


# --------------------------------------------------------------------------- #
# Zona raw (JSON append-only, particionada por ingestion_date)
# --------------------------------------------------------------------------- #
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
