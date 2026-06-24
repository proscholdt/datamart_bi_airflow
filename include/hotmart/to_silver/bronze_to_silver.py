"""bronze → silver (hotmart).

sales: lê SÓ a janela do bronze (watermark silver − lookback), dedup por transaction,
MERGE por transaction. (Limpezas/regras de negócio entram aqui se surgirem.)
subscriptions: pass-through (overwrite) do snapshot.
"""
import polars as pl

from common.delta_io import (
    merge_delta, overwrite_delta, scan_window, window_start, table_exists, storage_options,
)
from hotmart_meta import (
    SALES, SUBS, bronze_uri, silver_uri, SALES_GRAIN, SALES_PREDICATE, LOOKBACK_DAYS,
)


def bronze_to_silver_sales():
    start = window_start(silver_uri(SALES), LOOKBACK_DAYS, col="data")
    df = scan_window(bronze_uri(SALES), "data", start).collect()
    if df.is_empty():
        print(f"janela vazia no bronze_sales (>= {start})")
        return
    df = df.unique(subset=SALES_GRAIN, keep="last")
    merge_delta(df, silver_uri(SALES), SALES_PREDICATE)
    print(f"✅ silver_sales: {df.height} linhas (>= {start})")


def bronze_to_silver_subs():
    if not table_exists(bronze_uri(SUBS)):
        print("bronze_subscriptions inexistente; pula silver")
        return
    df = pl.read_delta(bronze_uri(SUBS), storage_options=storage_options())
    overwrite_delta(df, silver_uri(SUBS))
    print(f"✅ silver_subscriptions: {df.height} linhas (overwrite)")
