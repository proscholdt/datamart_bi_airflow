"""raw → bronze (hotmart).

sales: tipa a raw (ms→DATE em data/approved_date/warranty; casts; ids Utf8), dedup
por transaction e MERGE por transaction (upsert: status atualiza a linha).
subscriptions: snapshot → overwrite (flatten genérico; sem dado real hoje).
"""
import polars as pl

from common.delta_io import merge_delta, overwrite_delta
from hotmart_meta import (
    SALES, SUBS, bronze_uri, SALES_GRAIN, SALES_PREDICATE,
    SALES_DATE_COLS, SALES_FLOAT, SALES_INT, SALES_STR,
)
from to_raw.raw_io import read_raw_latest


def _typed_sales(df):
    casts = []
    for out_col, ms_col in SALES_DATE_COLS.items():
        if ms_col in df.columns:
            casts.append(pl.from_epoch(pl.col(ms_col), time_unit="ms").dt.date().alias(out_col))
    for c in SALES_FLOAT:
        if c in df.columns:
            casts.append(pl.col(c).cast(pl.Float64, strict=False))
    for c in SALES_INT:
        if c in df.columns:
            casts.append(pl.col(c).cast(pl.Int64, strict=False))
    for c in SALES_STR:
        if c in df.columns:
            casts.append(pl.col(c).cast(pl.Utf8, strict=False))
    if "is_subscription" in df.columns:
        casts.append(pl.col("is_subscription").cast(pl.Boolean, strict=False))
    df = df.with_columns(casts)
    drop = [c for c in df.columns if c.endswith("_ms")]
    return df.drop(drop) if drop else df


def raw_to_bronze_sales():
    df = read_raw_latest(SALES)
    if df.is_empty():
        print("raw vazio (sales); nada a mesclar no bronze")
        return
    df = _typed_sales(df).unique(subset=SALES_GRAIN, keep="last")
    merge_delta(df, bronze_uri(SALES), SALES_PREDICATE)
    print(f"✅ bronze_sales: {df.height} linhas mescladas")


def raw_to_bronze_subs():
    df = read_raw_latest(SUBS)
    if df.is_empty():
        print("raw vazio (subscriptions); bronze não criado")
        return
    overwrite_delta(df, bronze_uri(SUBS))
    print(f"✅ bronze_subscriptions: {df.height} linhas (overwrite)")
