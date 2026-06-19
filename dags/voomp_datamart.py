"""DAG do data mart Voomp (arquitetura medallion: bronze -> silver -> gold).

Substitui a orquestração antiga por subprocess (Orchestrator_master.py + os
*_orchestrator_*.py) por um DAG do Airflow, com paralelismo, retries e logs
por task. Cada task executa um dos scripts originais (verbatim) via runpy.

Dois ramos independentes correm em paralelo:
  - Vendas:    espera arquivo -> bronze->silver -> (4 dims + f_vendas em
               paralelo) -> arquiva o f_vendas na silver; em paralelo arquiva
               o xlsx de origem no bronze.
  - Projeções: espera arquivo -> bronze->silver -> silver->gold.

Agendamento: Seg-Sex ao meio-dia (BRT), alinhado à janela de hibernação do
Deployment. O WasbPrefixSensor faz uma checagem rápida do arquivo no bronze.

Execução: Deployment com CeleryExecutor (Development + hibernação). O
`executor_config` de pods abaixo só age sob KubernetesExecutor — é ignorado
sem efeito pelo Celery/LocalExecutor, então fica pronto caso troque depois.
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
from airflow.providers.microsoft.azure.sensors.wasb import WasbPrefixSensor
from airflow.utils.trigger_rule import TriggerRule
from kubernetes.client import models as k8s

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")
VOOMP_ROOT = Path(AIRFLOW_HOME) / "include" / "voomp"

# Container do bronze observado pelos sensores (mesmo default de voomp_config.py)
BRONZE_CONTAINER = os.getenv("VOOMP_CONTAINER_BRONZE", "datamartbibronze")


# --------------------------------------------------------------------------- #
# Execução dos scripts originais
# --------------------------------------------------------------------------- #
def run_script(script_relpath: str) -> None:
    """Executa um script de include/voomp como se fosse '__main__'.

    Coloca include/voomp no sys.path para que os scripts possam fazer
    `from voomp_config import ...`. Qualquer exceção propaga e FALHA a task
    (os scripts foram ajustados para re-levantar erros).
    """
    if str(VOOMP_ROOT) not in sys.path:
        sys.path.insert(0, str(VOOMP_ROOT))
    runpy.run_path(str(VOOMP_ROOT / script_relpath), run_name="__main__")


def k8s_pod(mem_request: str, mem_limit: str, cpu_request: str = "250m") -> dict:
    """executor_config de pod para o KubernetesExecutor (ajuste ao volume real)."""
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


# Polars carrega o dataset inteiro em memória (entrada + saída em BytesIO).
MEM_DEFAULT = k8s_pod("512Mi", "2Gi")
MEM_HEAVY = k8s_pod("1Gi", "4Gi")  # leitura de Excel + hash linha-a-linha / fato grande


def voomp_task(task_id: str, script_relpath: str, executor_config=None, **kwargs) -> PythonOperator:
    return PythonOperator(
        task_id=task_id,
        python_callable=run_script,
        op_kwargs={"script_relpath": script_relpath},
        executor_config=executor_config or MEM_DEFAULT,
        **kwargs,
    )


with DAG(
    dag_id="voomp_datamart",
    description="Data mart Voomp: bronze -> silver -> gold (Azure Blob + Polars)",
    # Roda Seg-Sex ao meio-dia (BRT), alinhado à janela acordada do Deployment
    # (hibernação): o arquivo já chegou (1x/dia), o sensor confere e processa.
    schedule="0 12 * * 1-5",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,  # tasks movem/deletam blobs: 2 runs simultâneos corromperiam o estado
    default_args={"retries": 1},
    tags=["voomp", "medallion", "azure-blob"],
) as dag:

    # --- Sensores de entrada (event-driven) ------------------------------ #
    # mode="reschedule": libera o worker entre os pokes; o soft_fail é respeitado
    # no timeout (no Airflow 3 o soft_fail não funciona com deferrable=True).
    wait_voomp_files = WasbPrefixSensor(
        task_id="wait_voomp_files",
        container_name=BRONZE_CONTAINER,
        prefix="source_voomp/voomp/",
        wasb_conn_id="wasb_default",
        mode="reschedule",
        poke_interval=120,
        timeout=60 * 30,
        soft_fail=True,  # sem arquivo na janela -> pula o ramo (não falha o run)
    )

    wait_projetadas_files = WasbPrefixSensor(
        task_id="wait_projetadas_files",
        container_name=BRONZE_CONTAINER,
        prefix="source_voomp/projetadas_voomp/",
        wasb_conn_id="wasb_default",
        mode="reschedule",
        poke_interval=120,
        timeout=60 * 30,
        soft_fail=True,  # projeção é opcional -> ausência pula o ramo
    )

    # --- Ramo VENDAS ----------------------------------------------------- #
    bronze_voomp_to_silver = voomp_task(
        "bronze_voomp_to_silver",
        "1_bronze/1_voomp_bronzeTOsilver.py",
        executor_config=MEM_HEAVY,
    )

    dim_afiliado = voomp_task(
        "dim_afiliado_silver_to_gold",
        "2_silver/1_TO_gold/1_dim_afiliado_silverTOgold.py",
    )
    dim_cliente = voomp_task(
        "dim_cliente_silver_to_gold",
        "2_silver/1_TO_gold/2_dim_cliente_silverTOgold.py",
    )
    dim_oferta = voomp_task(
        "dim_oferta_silver_to_gold",
        "2_silver/1_TO_gold/3_dim_oferta_silverTOgold.py",
    )
    dim_produto = voomp_task(
        "dim_produto_silver_to_gold",
        "2_silver/1_TO_gold/4_dim_produto_silverTOgold.py",
    )
    f_vendas_silver_to_gold = voomp_task(
        "f_vendas_silver_to_gold",
        "2_silver/1_TO_gold/6_f_vendas_silverTOgold.py",
        executor_config=MEM_HEAVY,
    )

    silver_to_gold_tasks = [
        dim_afiliado,
        dim_cliente,
        dim_oferta,
        dim_produto,
        f_vendas_silver_to_gold,
    ]

    # Arquiva o xlsx de origem no bronze (depois que o bronze->silver leu).
    # download->upload->delete NÃO é idempotente -> sem retry.
    archive_bronze_voomp = voomp_task(
        "archive_bronze_voomp",
        "1_bronze/2_voomp_TO_carregados.py",
        retries=0,
    )

    # Arquiva o f_vendas da silver SÓ depois que as 5 tasks acima o leram.
    archive_silver_f_vendas = voomp_task(
        "archive_silver_f_vendas",
        "2_silver/2_TO_carregados/8_f_vendas_TO_carregados.py",
        retries=0,
    )

    # --- Ramo PROJEÇÕES (paralelo) -------------------------------------- #
    projetadas_bronze_to_silver = voomp_task(
        "projetadas_bronze_to_silver",
        "1_bronze/00_projetadas_bronzeTosilver.py",
    )
    projetadas_silver_to_gold = voomp_task(
        "projetadas_silver_to_gold",
        "2_silver/1_TO_gold/00_projetadas_silverTOgold.py",
    )

    # --- Nó final -------------------------------------------------------- #
    pipeline_done = EmptyOperator(
        task_id="pipeline_done",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # --- Dependências ---------------------------------------------------- #
    wait_voomp_files >> bronze_voomp_to_silver
    bronze_voomp_to_silver >> silver_to_gold_tasks
    bronze_voomp_to_silver >> archive_bronze_voomp
    silver_to_gold_tasks >> archive_silver_f_vendas

    wait_projetadas_files >> projetadas_bronze_to_silver >> projetadas_silver_to_gold

    [archive_silver_f_vendas, archive_bronze_voomp, projetadas_silver_to_gold] >> pipeline_done
