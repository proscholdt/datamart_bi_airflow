"""DAG do data mart Facebook (Meta) — medallion em Delta Lake.

Por entidade (campaign / adset / ad), 3 cadeias paralelas:
    ingest_api → raw_to_bronze → bronze_to_silver → silver_to_gold (fato + dim)

- ingest: extrai a janela [watermark(bronze) − lookback ~10d, D-1] da Graph API
  (restatement da Meta: conversões/ROAS mudam retroativamente), valida in-task
  (staging efêmero) e grava JSON na zona RAW (append-only, particionada por
  ingestion_date). Sem stage container, sem pastas carregados.
- raw→bronze e bronze→silver: tabelas Delta, MERGE por grão (data + id).
- silver→gold: FATO `f_<ent>` (delete+append por janela, particionado por data)
  + DIM `dim_<ent>` (MERGE por id, SCD-1).

Backfill: setar FB_LOAD_START_DATE / FB_LOAD_END_DATE no ambiente.
Agenda: Seg-Sex meio-dia (BRT). compact/vacuum ficam na DAG `maintenance_datamart`.
"""
from __future__ import annotations

import os
import runpy
import sys
from pathlib import Path

import pendulum

from airflow.models.dag import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")
INCLUDE_ROOT = Path(AIRFLOW_HOME) / "include"
FB_ROOT = INCLUDE_ROOT / "facebook"


def _ensure_path() -> None:
    # include/ -> `from common.delta_io import ...`; include/facebook/ -> `import fb_*`.
    for p in (str(INCLUDE_ROOT), str(FB_ROOT)):
        if p not in sys.path:
            sys.path.insert(0, p)


def run_ingest(script_relpath: str) -> None:
    """Roda um script de ingestão (cargaDiaria) como __main__ via runpy."""
    _ensure_path()
    runpy.run_path(str(FB_ROOT / script_relpath), run_name="__main__")


def run_transform(func_name: str, entity: str) -> None:
    """Chama fb_transforms.<func_name>(entity) (raw_to_bronze / bronze_to_silver / silver_to_gold)."""
    _ensure_path()
    import fb_transforms

    getattr(fb_transforms, func_name)(entity)


# entidade -> script de ingestão (API → RAW)
ENTITIES = {
    "camp": "1_bronze/cargaDiaria/1_diaAdia_face_camp.py",
    "adset": "1_bronze/cargaDiaria/2_diaAdia_face_adset.py",
    "ad": "1_bronze/cargaDiaria/3_diaAdia_face_ad.py",
}


with DAG(
    dag_id="facebook_datamart",
    description="Data mart Facebook: API → raw → bronze → silver → gold (Delta Lake)",
    schedule="0 12 * * 1-5",  # Seg-Sex meio-dia (BRT)
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,  # MERGE/delete em Delta: 1 run por vez evita conflito de commit
    default_args={"retries": 1},
    tags=["facebook", "meta", "delta", "medallion"],
) as dag:

    pipeline_done = EmptyOperator(task_id="pipeline_done")

    for ent, ingest_script in ENTITIES.items():
        ingest = PythonOperator(
            task_id=f"{ent}_ingest_api",
            python_callable=run_ingest,
            op_kwargs={"script_relpath": ingest_script},
        )
        raw_to_bronze = PythonOperator(
            task_id=f"{ent}_raw_to_bronze",
            python_callable=run_transform,
            op_kwargs={"func_name": "raw_to_bronze", "entity": ent},
        )
        bronze_to_silver = PythonOperator(
            task_id=f"{ent}_bronze_to_silver",
            python_callable=run_transform,
            op_kwargs={"func_name": "bronze_to_silver", "entity": ent},
        )
        silver_to_gold = PythonOperator(
            task_id=f"{ent}_silver_to_gold",
            python_callable=run_transform,
            op_kwargs={"func_name": "silver_to_gold", "entity": ent},
        )

        ingest >> raw_to_bronze >> bronze_to_silver >> silver_to_gold >> pipeline_done
