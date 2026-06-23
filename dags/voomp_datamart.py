"""DAG do data mart Voomp — medallion em Delta Lake (overwrite snapshot).

Excel dropado na INBOX (stage) → ingest valida + copia p/ RAW (histórico por
ingestion_date) → bronze (Delta overwrite) → silver (MD5 + regras) → gold
(f_vendas + 4 dims). Ramo projeções opcional. SEM incremental/lookback: cada
drop é um snapshot completo; histórico via versões Delta + partições raw.

    wait_voomp_files → ingest_vendas → bronze_vendas → silver_vendas
                                                          ├→ gold_f_vendas
                                                          └→ gold_dim_{afiliado,cliente,oferta,produto}
    wait_projetadas_files → ingest_projetadas → bronze_projetadas → gold_projetadas   (opcional)

Agenda: Seg-Sex meio-dia (BRT). compact/vacuum na DAG `maintenance_datamart`.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pendulum

from airflow.models.dag import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.microsoft.azure.sensors.wasb import WasbPrefixSensor
from airflow.utils.trigger_rule import TriggerRule
from kubernetes.client import models as k8s

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")
INCLUDE_ROOT = Path(AIRFLOW_HOME) / "include"
VOOMP_ROOT = INCLUDE_ROOT / "voomp"

# Inbox de chegada do Excel (sensor observa aqui). Stage segue como drop zone.
INBOX_CONTAINER = os.getenv("VOOMP_CONTAINER_STAGE", "datamartbistage")


def _ensure_path() -> None:
    for p in (str(INCLUDE_ROOT), str(VOOMP_ROOT)):
        if p not in sys.path:
            sys.path.insert(0, p)


# func_name -> módulo do estágio (pastas to_stage / to_bronze / to_silver / to_gold)
_FUNC_MODULES = {
    "ingest_vendas": "to_stage.ingest",
    "ingest_projetadas": "to_stage.ingest",
    "bronze_vendas": "to_bronze.bronze",
    "bronze_projetadas": "to_bronze.bronze",
    "silver_vendas": "to_silver.silver",
    "gold_f_vendas": "to_gold.gold",
    "gold_dim": "to_gold.gold",
    "gold_projetadas": "to_gold.gold",
}


def run_transform(func_name: str, name: str | None = None) -> None:
    """Chama <modulo_do_estagio>.<func_name>([name]).

    Assinatura SEM **kwargs de propósito: com **kwargs o PythonOperator (Airflow 3)
    injetaria o contexto inteiro (dag/ti/ds...) e repassaria às funções de transform.
    """
    _ensure_path()
    import importlib

    fn = getattr(importlib.import_module(_FUNC_MODULES[func_name]), func_name)
    fn(name) if name is not None else fn()


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
MEM_HEAVY = k8s_pod("1Gi", "4Gi")       # Excel + hash linha-a-linha / fato grande


with DAG(
    dag_id="voomp_datamart",
    description="Data mart Voomp: Excel → raw → bronze → silver → gold (Delta Lake, overwrite)",
    schedule="0 12 * * 1-5",  # Seg-Sex meio-dia (BRT)
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,  # overwrite Delta: 1 run por vez
    default_args={"retries": 1},
    tags=["voomp", "delta", "medallion", "azure-blob"],
) as dag:

    def t(task_id: str, func: str, mem=None, **op) -> PythonOperator:
        return PythonOperator(
            task_id=task_id,
            python_callable=run_transform,
            op_kwargs={"func_name": func, **op},
            executor_config=mem or MEM_DEFAULT,
        )

    # --- Sensores: arquivo dropado na inbox (stage) -------------------- #
    wait_voomp_files = WasbPrefixSensor(
        task_id="wait_voomp_files",
        container_name=INBOX_CONTAINER,
        prefix="source_voomp/voomp/",
        wasb_conn_id="wasb_default",
        mode="reschedule",
        poke_interval=120,
        timeout=60 * 30,
        soft_fail=True,  # sem Excel na janela -> dia ocioso (pula o ramo)
    )
    wait_projetadas_files = WasbPrefixSensor(
        task_id="wait_projetadas_files",
        container_name=INBOX_CONTAINER,
        prefix="source_voomp/projetadas_voomp/",
        wasb_conn_id="wasb_default",
        mode="reschedule",
        poke_interval=120,
        timeout=60 * 30,
        soft_fail=True,  # projeção é opcional
    )

    # --- Ramo VENDAS --------------------------------------------------- #
    ingest_vendas = t("ingest_vendas", "ingest_vendas")
    bronze_vendas = t("bronze_vendas", "bronze_vendas", mem=MEM_HEAVY)  # lê o Excel
    silver_vendas = t("silver_vendas", "silver_vendas", mem=MEM_HEAVY)  # MD5 linha-a-linha
    gold_f_vendas = t("gold_f_vendas", "gold_f_vendas", mem=MEM_HEAVY)  # fato completo
    dims = [
        t(f"gold_{n}", "gold_dim", name=n)
        for n in ("dim_afiliado", "dim_cliente", "dim_oferta", "dim_produto")
    ]

    # --- Ramo PROJEÇÕES (opcional) ------------------------------------ #
    ingest_projetadas = t("ingest_projetadas", "ingest_projetadas")
    bronze_projetadas = t("bronze_projetadas", "bronze_projetadas")
    gold_projetadas = t("gold_projetadas", "gold_projetadas")

    pipeline_done = EmptyOperator(
        task_id="pipeline_done",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # --- Dependências -------------------------------------------------- #
    wait_voomp_files >> ingest_vendas >> bronze_vendas >> silver_vendas
    silver_vendas >> gold_f_vendas >> pipeline_done
    silver_vendas >> dims >> pipeline_done

    wait_projetadas_files >> ingest_projetadas >> bronze_projetadas >> gold_projetadas >> pipeline_done
