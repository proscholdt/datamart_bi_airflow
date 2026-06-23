"""Transforms Delta do data mart voomp (overwrite snapshot; sem lookback/incremental).

Funções chamadas pela DAG via PythonOperator:
  vendas:     ingest_vendas → bronze_vendas → silver_vendas → [gold_f_vendas, gold_dim(*)]
  projeções:  ingest_projetadas → bronze_projetadas → gold_projetadas   (opcional)

Cada camada bronze/silver/gold usa overwrite Delta — a tabela lê o snapshot mais
recente e o _delta_log guarda as versões anteriores (histórico/time travel).
"""
import hashlib
import io
import os
from datetime import date

import polars as pl

from voomp_config import get_container_client
from common.delta_io import overwrite_delta, storage_options, table_exists
from voomp_meta import (
    INBOX_VENDAS, INBOX_PROJETADAS, RAW_VENDAS, RAW_PROJETADAS,
    SHEET_VENDAS, VENDAS_COLS_OBRIGATORIAS, IDS_REMOVIDOS,
    F_VENDAS_COLS, DIM_DEFS,
    bronze_vendas_uri, silver_vendas_uri, f_vendas_uri, dim_uri,
    bronze_projetadas_uri, projetadas_uri,
)

MESES = {
    "jan": "01", "fev": "02", "mar": "03", "abr": "04", "mai": "05", "jun": "06",
    "jul": "07", "ago": "08", "set": "09", "out": "10", "nov": "11", "dez": "12",
}


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _ingestion_date():
    return date.today().isoformat()


def _inbox_files(prefix, exts):
    c = get_container_client("stage")
    out = []
    for b in c.list_blobs(name_starts_with=prefix + "/"):
        nome = os.path.basename(b.name)
        if nome.lower().endswith(exts) and not nome.startswith(("+", "~")):
            out.append(b.name)
    return out


def _read_any(buf, name):
    return pl.read_csv(buf) if name.lower().endswith(".csv") else pl.read_excel(buf)


def _latest_raw_file(raw_prefix, exts):
    """(blob_name) do arquivo na partição ingestion_date mais recente da RAW; None se vazio."""
    c = get_container_client("raw")
    parts = {}
    for b in c.list_blobs(name_starts_with=raw_prefix + "/"):
        rel = b.name[len(raw_prefix) + 1:]
        seg = rel.split("/")[0]
        if seg.startswith("ingestion_date=") and b.name.lower().endswith(exts):
            parts.setdefault(seg, []).append(b.name)
    if not parts:
        return None
    return sorted(parts[max(parts)])[0]


def _gerar_hash(texto):
    if texto is None or texto != texto:  # None ou NaN
        texto = ""
    return hashlib.md5(str(texto).encode("utf-8")).hexdigest()


def _mes_ano_para_data(mes_ano):
    if mes_ano is None:
        return None
    mes_ano = mes_ano.strip()
    mes, ano = mes_ano.split("/")
    mes_num = MESES.get(mes.lower())
    return f"01/{mes_num}/{ano}" if mes_num else None


# --------------------------------------------------------------------------- #
# Ramo VENDAS
# --------------------------------------------------------------------------- #
def ingest_vendas():
    """Valida o Excel dropado na inbox, copia p/ RAW (ingestion_date) e limpa a inbox."""
    files = _inbox_files(INBOX_VENDAS, (".xlsx",))
    if not files:
        raise Exception(f"❌ Nenhum Excel de vendas na inbox stage/{INBOX_VENDAS}/")
    ing = _ingestion_date()
    raw_c = get_container_client("raw")
    stage_c = get_container_client("stage")
    for name in files:
        data_bytes = stage_c.download_blob(name).readall()
        # validação (staging): legível + aba esperada + colunas-chave + não-vazio
        df = pl.read_excel(io.BytesIO(data_bytes), sheet_name=SHEET_VENDAS)
        if df.height == 0:
            raise Exception(f"❌ Validação: {name} sem linhas")
        faltando = [c for c in VENDAS_COLS_OBRIGATORIAS if c not in df.columns]
        if faltando:
            raise Exception(f"❌ Validação: {name} faltando {faltando}")
        raw_name = f"{RAW_VENDAS}/ingestion_date={ing}/{os.path.basename(name)}"
        raw_c.upload_blob(name=raw_name, data=data_bytes, overwrite=True)
        print(f"📥 raw: {raw_name} ({df.height} linhas)")
    for name in files:
        stage_c.delete_blob(name)  # limpa a inbox (não re-disparar o sensor)
    print(f"✅ ingest_vendas: {len(files)} arquivo(s) → raw (ingestion_date={ing}); inbox limpa")


def bronze_vendas():
    """Lê o Excel mais recente da RAW (aba de vendas) e grava bronze Delta (overwrite)."""
    name = _latest_raw_file(RAW_VENDAS, (".xlsx",))
    if not name:
        raise Exception("❌ Nenhum Excel de vendas na RAW")
    buf = io.BytesIO()
    get_container_client("raw").download_blob(name).readinto(buf)
    buf.seek(0)
    df = pl.read_excel(buf, sheet_name=SHEET_VENDAS)
    overwrite_delta(df, bronze_vendas_uri())
    print(f"✅ bronze_vendas: {df.height} linhas (overwrite) de {name}")


def silver_vendas():
    """bronze → filtros + MD5 (ID_Cliente/ID_Afiliado) + remoção de IDs → silver Delta."""
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


# --------------------------------------------------------------------------- #
# Ramo PROJEÇÕES (opcional)
# --------------------------------------------------------------------------- #
def ingest_projetadas():
    files = _inbox_files(INBOX_PROJETADAS, (".xlsx", ".xls", ".csv"))
    if not files:
        print("ℹ️ Sem projetadas na inbox — ramo opcional pulado")
        return
    ing = _ingestion_date()
    raw_c = get_container_client("raw")
    stage_c = get_container_client("stage")
    for name in files:
        data_bytes = stage_c.download_blob(name).readall()
        df = _read_any(io.BytesIO(data_bytes), name)
        if df.height == 0:
            raise Exception(f"❌ Validação projetadas: {name} vazio")
        raw_name = f"{RAW_PROJETADAS}/ingestion_date={ing}/{os.path.basename(name)}"
        raw_c.upload_blob(name=raw_name, data=data_bytes, overwrite=True)
        print(f"📥 raw: {raw_name} ({df.height} linhas)")
    for name in files:
        stage_c.delete_blob(name)
    print(f"✅ ingest_projetadas: {len(files)} → raw (ingestion_date={ing})")


def bronze_projetadas():
    name = _latest_raw_file(RAW_PROJETADAS, (".xlsx", ".xls", ".csv"))
    if not name:
        print("ℹ️ Sem projetadas na RAW — pula bronze")
        return
    buf = io.BytesIO()
    get_container_client("raw").download_blob(name).readinto(buf)
    buf.seek(0)
    df = _read_any(buf, name)
    overwrite_delta(df, bronze_projetadas_uri())
    print(f"✅ bronze_projetadas: {df.height} linhas de {name}")


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
