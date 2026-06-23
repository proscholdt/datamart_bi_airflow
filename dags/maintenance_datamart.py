"""DAG de manutenção das tabelas Delta (separada das DAGs de carga).

Para cada tabela Delta (bronze/silver/gold dos data marts facebook e voomp):
  DeltaTable(uri).optimize.compact()  →  vacuum(retention_hours=168)

retention_hours=168 (7 dias) define SIMULTANEAMENTE a economia de storage e a
janela de time travel disponível. A zona raw (JSON/Excel) não é Delta → fora.

Agenda: semanal (sábado de madrugada, BRT), fora da janela das cargas.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pendulum

from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")
INCLUDE_ROOT = Path(AIRFLOW_HOME) / "include"

BRONZE = os.getenv("VOOMP_CONTAINER_BRONZE", "datamartbibronze")
SILVER = os.getenv("VOOMP_CONTAINER_SILVER", "datamartbisilver")
GOLD = os.getenv("VOOMP_CONTAINER_GOLD", "datamartbigold")
RETENTION_HOURS = int(os.getenv("DELTA_VACUUM_RETENTION_HOURS", "168"))

# (container, path) de TODA tabela Delta gerenciada.
TABLES: list[tuple[str, str]] = []
for ent in ("camp", "adset", "ad"):
    TABLES += [
        (BRONZE, f"facebook/bronze_{ent}"),
        (SILVER, f"facebook/silver_{ent}"),
        (GOLD, f"facebook/f_{ent}"),
        (GOLD, f"facebook/dim_{ent}"),
    ]
TABLES += [
    (BRONZE, "voomp/bronze_vendas"),
    (BRONZE, "voomp/bronze_projetadas"),
    (SILVER, "voomp/silver_vendas"),
    (GOLD, "voomp/f_vendas"),
    (GOLD, "voomp/dim_afiliado"),
    (GOLD, "voomp/dim_cliente"),
    (GOLD, "voomp/dim_oferta"),
    (GOLD, "voomp/dim_produto"),
    (GOLD, "voomp/projetadas"),
]


def run_maintain(container: str, path: str) -> None:
    if str(INCLUDE_ROOT) not in sys.path:
        sys.path.insert(0, str(INCLUDE_ROOT))
    from common.delta_io import delta_uri, maintain, table_exists

    uri = delta_uri(container, path)
    if not table_exists(uri):
        print(f"ℹ️ tabela inexistente, pula: {uri}")
        return
    maintain(uri, retention_hours=RETENTION_HOURS)
    print(f"🧹 manutenção OK (compact + vacuum {RETENTION_HOURS}h): {uri}")


with DAG(
    dag_id="maintenance_datamart",
    description="Compact + vacuum das tabelas Delta (facebook + voomp)",
    schedule="0 3 * * 6",  # sábado 03:00 (BRT)
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["maintenance", "delta", "vacuum", "compact"],
) as dag:

    for container, path in TABLES:
        PythonOperator(
            task_id=f"maintain_{path.replace('/', '_')}",
            python_callable=run_maintain,
            op_kwargs={"container": container, "path": path},
        )
