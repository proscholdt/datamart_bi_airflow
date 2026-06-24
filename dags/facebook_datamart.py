"""DAG do data mart Facebook (Meta) — medallion em Delta Lake.

Por entidade (campaign / adset / ad), 3 cadeias paralelas:
    ingest_api → stage_to_raw → raw_to_bronze → bronze_to_silver → silver_to_gold

- ingest: extrai a janela [watermark(bronze) − lookback ~10d, D-1] da Graph API
  (restatement da Meta: conversões/ROAS mudam retroativamente), valida e grava o
  JSON no STAGE (zona de pouso, pastas permanentes).
- stage_to_raw: promove os JSON do stage p/ a RAW (append-only, ingestion_date) e
  limpa o stage (só arquivos; pastas ficam).
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
from kubernetes.client import models as k8s

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


# func_name -> módulo do estágio (pastas to_raw / to_bronze / to_silver / to_gold)
_TRANSFORM_MODULES = {
    "stage_to_raw": "to_raw.raw_io",
    "raw_to_bronze": "to_bronze.raw_to_bronze",
    "bronze_to_silver": "to_silver.bronze_to_silver",
    "silver_to_gold": "to_gold.silver_to_gold",
}


def run_transform(func_name: str, entity: str) -> None:
    """Chama <modulo_do_estagio>.<func_name>(entity)."""
    _ensure_path()
    import importlib

    module = importlib.import_module(_TRANSFORM_MODULES[func_name])
    getattr(module, func_name)(entity)


# --- Recursos por task: SÓ valem sob KubernetesExecutor (request=MIN, limit=MAX).
#     No Astro Executor / LocalExecutor são ignorados — ficam prontos p/ K8s.
def k8s_pod(mem_request: str, mem_limit: str, cpu_request: str = "250m") -> dict:
    return {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        resources=k8s.V1ResourceRequirements(
                            requests={"memory": mem_request, "cpu": cpu_request},
                            limits={"memory": mem_limit},
                        ),
                    )
                ]
            )
        )
    }


MEM_DEFAULT = k8s_pod("512Mi", "2Gi")   # min 512Mi / max 2Gi
MEM_HEAVY = k8s_pod("1Gi", "4Gi")       # ad: pull grande + resolução de criativo


# entidade -> script de ingestão (API → STAGE), em to_stage/
ENTITIES = {
    "camp": "to_stage/1_diaAdia_face_camp.py",
    "adset": "to_stage/2_diaAdia_face_adset.py",
    "ad": "to_stage/3_diaAdia_face_ad.py",
}


with DAG(
    dag_id="facebook_datamart",
    description="Data mart Facebook: API → stage → raw → bronze → silver → gold (Delta Lake)",
    schedule="0 12 * * 1-5",  # Seg-Sex meio-dia (BRT)
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,  # MERGE/delete em Delta: 1 run por vez evita conflito de commit
    default_args={"retries": 1},
    tags=["facebook", "meta", "delta", "medallion"],
) as dag:

    pipeline_done = EmptyOperator(task_id="pipeline_done")

    for ent, ingest_script in ENTITIES.items():
        mem = MEM_HEAVY if ent == "ad" else MEM_DEFAULT  # 'ad' é o mais pesado
        ingest = PythonOperator(
            task_id=f"{ent}_ingest_api",
            python_callable=run_ingest,
            op_kwargs={"script_relpath": ingest_script},
            executor_config=mem,
        )
        stage_to_raw = PythonOperator(
            task_id=f"{ent}_stage_to_raw",
            python_callable=run_transform,
            op_kwargs={"func_name": "stage_to_raw", "entity": ent},
            executor_config=mem,
        )
        raw_to_bronze = PythonOperator(
            task_id=f"{ent}_raw_to_bronze",
            python_callable=run_transform,
            op_kwargs={"func_name": "raw_to_bronze", "entity": ent},
            executor_config=mem,
        )
        bronze_to_silver = PythonOperator(
            task_id=f"{ent}_bronze_to_silver",
            python_callable=run_transform,
            op_kwargs={"func_name": "bronze_to_silver", "entity": ent},
            executor_config=mem,
        )
        silver_to_gold = PythonOperator(
            task_id=f"{ent}_silver_to_gold",
            python_callable=run_transform,
            op_kwargs={"func_name": "silver_to_gold", "entity": ent},
            executor_config=mem,
        )

        ingest >> stage_to_raw >> raw_to_bronze >> bronze_to_silver >> silver_to_gold >> pipeline_done
