"""STAGE do voomp — ingestão: valida o Excel dropado na inbox (stage), copia
p/ RAW (ingestion_date) e limpa a inbox. Vendas (obrigatório) e Projeções (opcional)."""
import io
import os

import polars as pl

from voomp_config import get_container_client
from voomp_meta import (
    INBOX_VENDAS, INBOX_PROJETADAS, RAW_VENDAS, RAW_PROJETADAS,
    SHEET_VENDAS, VENDAS_COLS_OBRIGATORIAS,
)
from to_raw.raw_io import write_raw, read_any, ingestion_date


def _inbox_files(prefix, exts):
    c = get_container_client("stage")
    out = []
    for b in c.list_blobs(name_starts_with=prefix + "/"):
        nome = os.path.basename(b.name)
        if nome.lower().endswith(exts) and not nome.startswith(("+", "~")):
            out.append(b.name)
    return out


def ingest_vendas():
    """Valida o Excel dropado na inbox, copia p/ RAW (ingestion_date) e limpa a inbox."""
    files = _inbox_files(INBOX_VENDAS, (".xlsx",))
    if not files:
        raise Exception(f"❌ Nenhum Excel de vendas na inbox stage/{INBOX_VENDAS}/")
    ing = ingestion_date()
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
        write_raw(RAW_VENDAS, os.path.basename(name), data_bytes, ing)
        print(f"   {name}: {df.height} linhas")
    for name in files:
        stage_c.delete_blob(name)  # limpa a inbox (não re-disparar o sensor)
    print(f"✅ ingest_vendas: {len(files)} arquivo(s) → raw (ingestion_date={ing}); inbox limpa")


def ingest_projetadas():
    files = _inbox_files(INBOX_PROJETADAS, (".xlsx", ".xls", ".csv"))
    if not files:
        print("ℹ️ Sem projetadas na inbox — ramo opcional pulado")
        return
    ing = ingestion_date()
    stage_c = get_container_client("stage")
    for name in files:
        data_bytes = stage_c.download_blob(name).readall()
        df = read_any(io.BytesIO(data_bytes), name)
        if df.height == 0:
            raise Exception(f"❌ Validação projetadas: {name} vazio")
        write_raw(RAW_PROJETADAS, os.path.basename(name), data_bytes, ing)
        print(f"   {name}: {df.height} linhas")
    for name in files:
        stage_c.delete_blob(name)
    print(f"✅ ingest_projetadas: {len(files)} → raw (ingestion_date={ing})")
