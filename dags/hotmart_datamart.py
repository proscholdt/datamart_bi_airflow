"""DAG do data mart Hotmart — medallion em Delta Lake (incremental por MERGE).

Dois ramos:
  VENDAS:       ingest_sales (sales/history) → stage → raw → bronze → silver → gold
                (f_vendas grão=transaction, MERGE; + dim_cliente/produto/oferta/produtor)
  ASSINATURAS:  ingest_subscriptions → stage → raw → bronze → silver → gold
                (f_assinaturas, snapshot/overwrite — conta sem assinaturas hoje)

Incremental: janela = watermark(bronze.data) − lookback (90d, restatement de status),
fatiada de 90 em 90 dias (limite da API). compact/vacuum na DAG maintenance_datamart.

Credenciais: HOTMART_CLIENT_ID / HOTMART_CLIENT_SECRET (renomeie as chaves
'Client ID'/'Client Secret' do .env — têm espaço e o astro não carrega) no deployment.
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
from airflow.utils.trigger_rule import TriggerRule
from kubernetes.client import models as k8s

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")
INCLUDE_ROOT = Path(AIRFLOW_HOME) / "include"
HOT_ROOT = INCLUDE_ROOT / "hotmart"


def _ensure_path() -> None:
    for p in (str(INCLUDE_ROOT), str(HOT_ROOT)):
        if p not in sys.path:
            sys.path.insert(0, p)


def run_ingest(script_relpath: str) -> None:
    _ensure_path()
    runpy.run_path(str(HOT_ROOT / script_relpath), run_name="__main__")


def run_func(module: str, func: str, arg: str | None = None) -> None:
    """Chama <module>.<func>([arg]). Sem **kwargs (Airflow 3 injetaria o contexto)."""
    _ensure_path()
    import importlib

    fn = getattr(importlib.import_module(module), func)
    fn(arg) if arg is not None else fn()


# Recursos por task: SÓ valem sob KubernetesExecutor; inertes no Astro/Local.
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


MEM = k8s_pod("512Mi", "2Gi")


with DAG(
    dag_id="hotmart_datamart",
    description="Data mart Hotmart: API → stage → raw → bronze → silver → gold (Delta, incremental)",
    schedule="0 12 * * 1-5",  # Seg-Sex meio-dia (BRT)
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,  # MERGE em Delta: 1 run por vez evita conflito de commit
    default_args={"retries": 1},
    tags=["hotmart", "delta", "medallion", "api"],
) as dag:

    def ingest(task_id, script):
        return PythonOperator(task_id=task_id, python_callable=run_ingest,
                              op_kwargs={"script_relpath": script}, executor_config=MEM)

    def step(task_id, module, func, arg=None):
        return PythonOperator(task_id=task_id, python_callable=run_func,
                              op_kwargs={"module": module, "func": func, "arg": arg}, executor_config=MEM)

    # entidade desabilitada (skipped) não bloqueia o fim.
    pipeline_done = EmptyOperator(
        task_id="pipeline_done", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # --- VENDAS ---------------------------------------------------------- #
    s_ingest = ingest("ingest_sales", "to_stage/ingest_sales.py")
    s_raw = step("sales_stage_to_raw", "to_raw.raw_io", "stage_to_raw", arg="sales")
    s_bronze = step("sales_raw_to_bronze", "to_bronze.raw_to_bronze", "raw_to_bronze_sales")
    s_silver = step("sales_bronze_to_silver", "to_silver.bronze_to_silver", "bronze_to_silver_sales")
    s_gold = step("sales_silver_to_gold", "to_gold.silver_to_gold", "silver_to_gold_sales")

    # --- ASSINATURAS ----------------------------------------------------- #
    a_ingest = ingest("ingest_subscriptions", "to_stage/ingest_subscriptions.py")
    a_raw = step("subs_stage_to_raw", "to_raw.raw_io", "stage_to_raw", arg="subscriptions")
    a_bronze = step("subs_raw_to_bronze", "to_bronze.raw_to_bronze", "raw_to_bronze_subs")
    a_silver = step("subs_bronze_to_silver", "to_silver.bronze_to_silver", "bronze_to_silver_subs")
    a_gold = step("subs_silver_to_gold", "to_gold.silver_to_gold", "silver_to_gold_subs")

    s_ingest >> s_raw >> s_bronze >> s_silver >> s_gold >> pipeline_done
    a_ingest >> a_raw >> a_bronze >> a_silver >> a_gold >> pipeline_done
