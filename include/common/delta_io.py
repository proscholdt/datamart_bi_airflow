"""Helpers de I/O Delta Lake (delta-rs / Polars) sobre Azure Blob (ADLS Gen2 / HNS).

Centraliza credenciais e os padrões de escrita Delta usados pelos data marts:
- credenciais lidas da Connection `wasb_default` do Airflow (login=conta,
  password=chave), com fallback p/ env STORAGE_ACCOUNT_NAME/KEY (execução local).
  NUNCA hardcoded.
- URIs no formato az://<container>/<path> (delta-rs object_store Azure).
- MERGE (bronze/silver), overwrite (voomp) e delete+append por janela (gold).

A conta é HNS/ADLS Gen2; o delta-rs fala o endpoint blob com account_key.
"""
import os
from datetime import date, timedelta

import polars as pl
from deltalake import DeltaTable

try:  # nome da exceção pode variar entre versões do deltalake
    from deltalake.exceptions import TableNotFoundError
except Exception:  # pragma: no cover
    class TableNotFoundError(Exception):
        pass


def account_credentials():
    """(conta, chave) da Connection wasb_default; fallback p/ env STORAGE_ACCOUNT_*."""
    try:
        from airflow.models import Connection

        conn = Connection.get_connection_from_secrets("wasb_default")
        if conn.login and conn.password:
            return conn.login, conn.password
    except Exception:
        pass
    return os.getenv("STORAGE_ACCOUNT_NAME"), os.getenv("STORAGE_ACCOUNT_KEY")


def storage_options():
    """storage_options do delta-rs (account_name/account_key)."""
    name, key = account_credentials()
    if not name or not key:
        raise RuntimeError(
            "Credenciais Azure ausentes (Connection wasb_default / env STORAGE_ACCOUNT_*)"
        )
    return {"account_name": name, "account_key": key}


def delta_uri(container, path):
    """az://<container>/<path> — caminho de uma tabela Delta."""
    return f"az://{container}/{path.strip('/')}"


def _write_opts(partition_by):
    return {"partition_by": list(partition_by)} if partition_by else None


def _sanitize(df):
    """Ajustes p/ compatibilidade do Delta gerado:

    1. Colunas 100% nulas (dtype Null, não suportado pelo writer Delta) → Utf8.
    2. Datetime SEM timezone → marca como UTC (timestamp padrão). O delta-rs grava
       datetime naive com o feature 'timestampNtz' (reader v3), que o Synapse
       serverless NÃO lê (devolve 0 linhas). Marcar UTC mantém o valor de parede
       e usa o timestamp padrão (reader v1), legível pelo Synapse.
    """
    casts = []
    for c, dt in df.schema.items():
        if dt == pl.Null:
            casts.append(pl.col(c).cast(pl.Utf8))
        elif isinstance(dt, pl.Datetime) and dt.time_zone is None:
            casts.append(pl.col(c).dt.replace_time_zone("UTC"))
    return df.with_columns(casts) if casts else df


def table_exists(uri, opts=None):
    try:
        DeltaTable(uri, storage_options=opts or storage_options())
        return True
    except TableNotFoundError:
        return False


def read_watermark(uri, col="data"):
    """Máximo de `col` na tabela; None se a tabela não existe (1º run)."""
    opts = storage_options()
    if not table_exists(uri, opts):
        return None
    return (
        pl.scan_delta(uri, storage_options=opts)
        .select(pl.col(col).max().alias("m"))
        .collect()
        .item()
    )


def scan_window(uri, col, start):
    """LazyFrame da tabela filtrada por `col >= start` (pushdown de partição)."""
    return pl.scan_delta(uri, storage_options=storage_options()).filter(pl.col(col) >= start)


def window_start(target_uri, lookback_days, col="data", floor=date(1970, 1, 1)):
    """Início da janela a reprocessar: watermark(alvo) - lookback.

    Se o alvo não existe (1ª carga), devolve `floor` → processa TUDO da fonte.
    Em regime, devolve só os últimos ~lookback dias (janela de restatement).
    """
    wm = read_watermark(target_uri, col)
    if wm is None:
        return floor
    return wm - timedelta(days=lookback_days)


def merge_delta(df, uri, predicate, partition_by=None):
    """Upsert por `predicate` (update_all / insert_all). Cria a tabela no 1º run.

    A FONTE (`df`) precisa estar deduplicada pela chave do predicate, senão o
    delta-merge falha com chave de origem duplicada.
    """
    df = _sanitize(df)
    opts = storage_options()
    if not table_exists(uri, opts):
        df.write_delta(
            uri, mode="overwrite", storage_options=opts,
            delta_write_options=_write_opts(partition_by),
        )
        return
    (
        df.write_delta(
            uri,
            mode="merge",
            storage_options=opts,
            delta_merge_options={
                "predicate": predicate,
                "source_alias": "s",
                "target_alias": "t",
            },
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )


def overwrite_delta(df, uri, partition_by=None, schema_mode="overwrite"):
    """Snapshot completo (voomp): substitui a tabela; versões antigas no _delta_log.

    schema_mode="overwrite" permite o schema evoluir entre snapshots (ex.: Excel
    com colunas/tipos diferentes a cada drop).
    """
    df = _sanitize(df)
    opts = _write_opts(partition_by) or {}
    if schema_mode:
        opts["schema_mode"] = schema_mode
    df.write_delta(
        uri, mode="overwrite", storage_options=storage_options(),
        delta_write_options=opts or None,
    )


def delete_and_append(df, uri, delete_predicate, partition_by=None):
    """Padrão transacional da gold: apaga a janela e re-anexa os agregados.

    Mais barato que MERGE quando a partição inteira é trocada (delete mira
    partições via `delete_predicate`, ex.: "data >= '2026-06-01'").
    """
    df = _sanitize(df)
    opts = storage_options()
    if not table_exists(uri, opts):
        df.write_delta(
            uri, mode="overwrite", storage_options=opts,
            delta_write_options=_write_opts(partition_by),
        )
        return
    DeltaTable(uri, storage_options=opts).delete(delete_predicate)
    df.write_delta(
        uri, mode="append", storage_options=opts,
        delta_write_options=_write_opts(partition_by),
    )


def maintain(uri, retention_hours=168):
    """compact + vacuum de uma tabela Delta (DAG de manutenção).

    retention_hours define simultaneamente a economia de storage e a janela de
    time travel disponível.
    """
    opts = storage_options()
    dt = DeltaTable(uri, storage_options=opts)
    dt.optimize.compact()
    dt.vacuum(retention_hours=retention_hours, enforce_retention_duration=False, dry_run=False)
