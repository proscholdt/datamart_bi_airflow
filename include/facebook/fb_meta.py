"""Metadados das entidades do data mart facebook (grão, colunas, URIs Delta, janela).

Centraliza, por entidade (camp/adset/ad): o grão (chave do MERGE), as colunas
do fato e da dimensão, os caminhos das tabelas Delta (bronze/silver/gold) e a
janela da API. A I/O da zona raw fica em to_raw/raw_io.py.
"""
import os
from datetime import date, timedelta

from facebook_config import BRONZE, SILVER, GOLD
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
