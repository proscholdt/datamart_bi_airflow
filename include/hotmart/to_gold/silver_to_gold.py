"""silver → gold (hotmart).

sales: FATO f_vendas (grão = transaction, MERGE por transaction, valor_liquido
derivado) + 4 dimensões (MERGE por chave, SCD-1). Fato NÃO particionado por data
(coluna de partição Delta vem NULL no Synapse); datas como DATE.
subscriptions: f_assinaturas (overwrite do snapshot).
"""
import polars as pl

from common.delta_io import (
    merge_delta, overwrite_delta, scan_window, window_start, table_exists, storage_options,
)
from hotmart_meta import (
    SALES, SUBS, silver_uri, f_vendas_uri, f_assinaturas_uri, dim_uri,
    F_VENDAS_COLS, DIM_DEFS, SALES_PREDICATE, LOOKBACK_DAYS,
)


def silver_to_gold_sales():
    start = window_start(f_vendas_uri(), LOOKBACK_DAYS, col="data")
    sdf = scan_window(silver_uri(SALES), "data", start).collect()
    if sdf.is_empty():
        print(f"janela vazia no silver_sales (>= {start})")
        return

    # FATO (grão transaction): colunas do fato + valor_liquido (price_value − fee_total).
    cols = [c for c in F_VENDAS_COLS if c in sdf.columns]
    fact = sdf.select(cols).unique(subset=["transaction"], keep="last")
    if "price_value" in fact.columns and "fee_total" in fact.columns:
        fact = fact.with_columns(
            (pl.col("price_value").fill_null(0) - pl.col("fee_total").fill_null(0)).alias("valor_liquido")
        )
    merge_delta(fact, f_vendas_uri(), SALES_PREDICATE)
    print(f"✅ f_vendas: {fact.height} linhas (>= {start})")

    # DIMENSÕES (MERGE por chave, SCD-1).
    for name, (key, dcols) in DIM_DEFS.items():
        have = [c for c in dcols if c in sdf.columns]
        if key not in have:
            continue
        ddf = sdf.select(have).filter(pl.col(key).is_not_null()).unique(subset=[key], keep="last")
        merge_delta(ddf, dim_uri(name), f"t.{key} = s.{key}")
        print(f"✅ {name}: {ddf.height} linhas (MERGE por {key})")


def silver_to_gold_subs():
    if not table_exists(silver_uri(SUBS)):
        print("silver_subscriptions inexistente; pula gold")
        return
    df = pl.read_delta(silver_uri(SUBS), storage_options=storage_options())
    overwrite_delta(df, f_assinaturas_uri())
    print(f"✅ f_assinaturas: {df.height} linhas (overwrite)")
