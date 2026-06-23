"""bronze do voomp — tabulariza o snapshot mais recente da RAW → Delta (overwrite)."""
import polars as pl

from voomp_meta import RAW_VENDAS, RAW_PROJETADAS, SHEET_VENDAS, bronze_vendas_uri, bronze_projetadas_uri
from common.delta_io import overwrite_delta
from to_raw.raw_io import latest_raw_file, download_raw, read_any


def bronze_vendas():
    """Lê o Excel mais recente da RAW (aba de vendas) e grava bronze Delta (overwrite)."""
    name = latest_raw_file(RAW_VENDAS, (".xlsx",))
    if not name:
        raise Exception("❌ Nenhum Excel de vendas na RAW")
    df = pl.read_excel(download_raw(name), sheet_name=SHEET_VENDAS)
    overwrite_delta(df, bronze_vendas_uri())
    print(f"✅ bronze_vendas: {df.height} linhas (overwrite) de {name}")


def bronze_projetadas():
    name = latest_raw_file(RAW_PROJETADAS, (".xlsx", ".xls", ".csv"))
    if not name:
        print("ℹ️ Sem projetadas na RAW — pula bronze")
        return
    df = read_any(download_raw(name), name)
    overwrite_delta(df, bronze_projetadas_uri())
    print(f"✅ bronze_projetadas: {df.height} linhas de {name}")
