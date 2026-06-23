"""Transforms Delta do data mart facebook: raw→bronze, bronze→silver, silver→gold.

Cada função é parametrizada pela entidade (camp/adset/ad) e chamada pela DAG via
PythonOperator. Usa os helpers de common.delta_io + os metadados de fb_meta.

- raw→bronze: lê a partição raw recém-gravada, tipa, MERGE por grão (lookback embutido
  no fato de a raw conter a janela restatementada).
- bronze→silver: lê só a janela do bronze, dedup/conforma, MERGE por grão.
- silver→gold: FATO (delete+append por janela, particionado por data) + DIM (MERGE por id).
"""
import polars as pl

from common.delta_io import merge_delta, scan_window, window_start, delete_and_append
from fb_meta import (
    ENTITIES,
    LOOKBACK_DAYS,
    MEASURE_FLOAT,
    MEASURE_INT,
    bronze_uri,
    silver_uri,
    fact_uri,
    dim_uri,
    grain_predicate,
    dim_predicate,
    read_raw_latest,
)


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


def bronze_to_silver(entity):
    cfg = ENTITIES[entity]
    start = window_start(silver_uri(entity), LOOKBACK_DAYS)
    df = scan_window(bronze_uri(entity), "data", start).collect()
    if df.is_empty():
        print(f"janela vazia no bronze_{entity} (>= {start})")
        return
    # silver conformada: dedup por grão (limpeza/regra entram aqui se houver)
    df = df.unique(subset=cfg["grain"], keep="last")
    merge_delta(df, silver_uri(entity), grain_predicate(entity), partition_by=["data"])
    print(f"✅ silver_{entity}: {df.height} linhas (>= {start})")


def silver_to_gold(entity):
    cfg = ENTITIES[entity]

    # --- FATO (grão atual data+id, sem rollup): delete janela + append ---
    fstart = window_start(fact_uri(entity), LOOKBACK_DAYS)
    sdf = scan_window(silver_uri(entity), "data", fstart).collect()
    if not sdf.is_empty():
        fact_cols = cfg["grain"] + cfg["fk_cols"] + (MEASURE_FLOAT + MEASURE_INT)
        fact = (
            sdf.select([c for c in fact_cols if c in sdf.columns])
            .unique(subset=cfg["grain"], keep="last")
        )
        delete_and_append(fact, fact_uri(entity), f"data >= '{fstart}'", partition_by=["data"])
        print(f"✅ f_{entity}: {fact.height} linhas (janela >= {fstart})")

        # --- DIMENSÃO (SCD-1 por id): MERGE (atualiza atributos, insere novos) ---
        dcols = [c for c in cfg["dim_cols"] if c in sdf.columns]
        ddf = sdf.select(dcols).unique(subset=cfg["dim_key"], keep="last")
        merge_delta(ddf, dim_uri(entity), dim_predicate(entity))
        print(f"✅ {cfg['dim_name']}: {ddf.height} linhas (MERGE por id)")
    else:
        print(f"janela vazia no silver_{entity} (>= {fstart})")
