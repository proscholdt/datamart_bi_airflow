"""gold do voomp — fato f_vendas (grão ID Venda) + 4 dimensões + projeções.
Tudo overwrite Delta (snapshot mais recente; histórico via versões)."""
import polars as pl

from voomp_meta import (
    silver_vendas_uri, f_vendas_uri, dim_uri, F_VENDAS_COLS, DIM_DEFS,
    bronze_projetadas_uri, projetadas_uri,
)
from common.delta_io import overwrite_delta, storage_options, table_exists

MESES = {
    "jan": "01", "fev": "02", "mar": "03", "abr": "04", "mai": "05", "jun": "06",
    "jul": "07", "ago": "08", "set": "09", "out": "10", "nov": "11", "dez": "12",
}


def _mes_ano_para_data(mes_ano):
    if mes_ano is None:
        return None
    mes_ano = mes_ano.strip()
    mes, ano = mes_ano.split("/")
    mes_num = MESES.get(mes.lower())
    return f"01/{mes_num}/{ano}" if mes_num else None


def gold_f_vendas():
    """silver → seleção das colunas do fato (grão ID Venda) → f_vendas Delta (overwrite)."""
    df = pl.read_delta(silver_vendas_uri(), storage_options=storage_options())
    cols = [c for c in F_VENDAS_COLS if c in df.columns]
    overwrite_delta(df.select(cols), f_vendas_uri())
    print(f"✅ f_vendas: {df.height} linhas, {len(cols)} colunas")


def gold_dim(name):
    """silver → seleção + unique() → dimensão Delta (overwrite). name ∈ DIM_DEFS."""
    df = pl.read_delta(silver_vendas_uri(), storage_options=storage_options())
    cols = [c for c in DIM_DEFS[name] if c in df.columns]
    out = df.select(cols).unique()
    overwrite_delta(out, dim_uri(name))
    print(f"✅ {name}: {out.height} linhas")


def gold_projetadas():
    """bronze_projetadas → Mês→Data + limpeza de Valor → [Data, Valor] Delta (overwrite)."""
    if not table_exists(bronze_projetadas_uri()):
        print("ℹ️ bronze_projetadas inexistente — pula gold")
        return
    df = pl.read_delta(bronze_projetadas_uri(), storage_options=storage_options())
    df = df.with_columns(
        pl.col("Mês").map_elements(_mes_ano_para_data, return_dtype=pl.Utf8).alias("Data")
    )
    df = df.with_columns(
        pl.col("Valor Total")
        .str.replace_all(r"[R$\s]", "")
        .str.replace_all(r"\.", "")
        .str.replace(",", ".")
        .cast(pl.Decimal(18, 2))
        .alias("Valor")
    )
    df = df.select(["Data", "Valor"])
    overwrite_delta(df, projetadas_uri())
    print(f"✅ projetadas: {df.height} linhas")
