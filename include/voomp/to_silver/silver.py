"""silver do voomp — bronze → filtros + IDs MD5 (ID_Cliente/ID_Afiliado) +
remoção de IDs de venda → Delta (overwrite). Recipe do hash preservado verbatim."""
import hashlib

import polars as pl

from voomp_meta import bronze_vendas_uri, silver_vendas_uri, IDS_REMOVIDOS
from common.delta_io import overwrite_delta, storage_options


def _gerar_hash(texto):
    if texto is None or texto != texto:  # None ou NaN
        texto = ""
    return hashlib.md5(str(texto).encode("utf-8")).hexdigest()


def silver_vendas():
    df = pl.read_delta(bronze_vendas_uri(), storage_options=storage_options())
    # apenas linhas com comprador + email não nulos
    df = df.filter(
        pl.col("Nome do comprador").is_not_null() & pl.col("Email do comprador").is_not_null()
    )
    # garante string (compatibilidade do hash)
    df = df.with_columns([
        pl.col("Nome do comprador").cast(pl.Utf8),
        pl.col("Email do comprador").cast(pl.Utf8),
        pl.col("Nome Afiliado").cast(pl.Utf8),
    ])
    # IDs MD5 (recipe verbatim do pipeline original)
    df = df.with_columns([
        (pl.col("Nome do comprador").fill_null("") + pl.col("Email do comprador").fill_null(""))
        .map_elements(_gerar_hash, return_dtype=pl.Utf8, skip_nulls=False).alias("ID_Cliente"),
        pl.col("Nome Afiliado").fill_null("")
        .map_elements(_gerar_hash, return_dtype=pl.Utf8, skip_nulls=False).alias("ID_Afiliado"),
    ])
    # remove IDs de venda específicos
    df = df.filter(~pl.col("ID Venda").is_in(IDS_REMOVIDOS))
    overwrite_delta(df, silver_vendas_uri())
    print(f"✅ silver_vendas: {df.height} linhas")
