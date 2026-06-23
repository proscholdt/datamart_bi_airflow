"""raw → bronze (facebook): tabulariza/tipa a raw recém-gravada e MERGE por grão.

O lookback fica embutido no fato de a raw conter a janela restatementada
(re-extraída pela ingestão). A fonte do MERGE é deduplicada pelo grão.
"""
import polars as pl

from common.delta_io import merge_delta
from fb_meta import ENTITIES, MEASURE_FLOAT, MEASURE_INT, bronze_uri, grain_predicate
from to_raw.raw_io import read_raw_latest


def _typed(df):
    """Tipa o DataFrame cru: `data` (Date) de date_start, medidas numéricas, ids Utf8."""
    out = df
    if "date_start" in out.columns:
        out = out.with_columns(
            pl.col("date_start").cast(pl.Utf8).str.to_date(strict=False).alias("data")
        )
    casts = []
    for col in MEASURE_FLOAT:
        if col in out.columns:
            casts.append(pl.col(col).cast(pl.Float64, strict=False))
    for col in MEASURE_INT:
        if col in out.columns:
            casts.append(pl.col(col).cast(pl.Int64, strict=False))
    for col in ("campaign_id", "adset_id", "ad_id"):
        if col in out.columns:
            casts.append(pl.col(col).cast(pl.Utf8, strict=False))
    return out.with_columns(casts) if casts else out


def raw_to_bronze(entity):
    cfg = ENTITIES[entity]
    df = read_raw_latest(entity)
    if df.is_empty():
        print(f"raw vazio para {entity}; nada a mesclar no bronze")
        return
    df = _typed(df)
    # dedup defensivo pelo grão (a FONTE do MERGE não pode ter chave duplicada)
    df = df.unique(subset=cfg["grain"], keep="last")
    merge_delta(df, bronze_uri(entity), grain_predicate(entity), partition_by=["data"])
    print(f"✅ bronze_{entity}: {df.height} linhas mescladas")
