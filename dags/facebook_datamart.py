"""DAG do data mart Facebook (medallion: bronze API → silver → gold).

Espelha o padrão do voomp_datamart, mas o BRONZE puxa da API do Facebook
(Graph API v19.0) em vez de esperar um arquivo. São 3 cadeias independentes
e paralelas — campaign / adset / ad — cada uma:

    ingest(API) → bronze→silver → silver→gold → dimensão
                       │                │
                       └→ arquiva       └→ arquiva  (carregados, em paralelo)

Carga de teste de 2 meses: setar FB_LOAD_START_DATE / FB_LOAD_END_DATE no
ambiente (ex.: no .env local). Os scripts cargaDiaria usam esse intervalo;
sem essas vars, fazem a carga incremental normal (última data + 1 → ontem).

Agendamento: Seg-Sex ao meio-dia (BRT), igual ao voomp (mesma janela de
hibernação do Deployment). Execução via runpy; exceções FALHAM a task.
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
FB_ROOT = Path(AIRFLOW_HOME) / "include" / "facebook"


def run_script(script_relpath: str) -> None:
    """Executa um script de include/facebook como '__main__' (via runpy).

    Coloca include/facebook no sys.path para os scripts poderem fazer
    `from facebook_config import ...`. Qualquer exceção propaga e FALHA a task.
    """
    if str(FB_ROOT) not in sys.path:
        sys.path.insert(0, str(FB_ROOT))
    runpy.run_path(str(FB_ROOT / script_relpath), run_name="__main__")


def fb_task(task_id: str, script_relpath: str, **kwargs) -> PythonOperator:
    return PythonOperator(
        task_id=task_id,
        python_callable=run_script,
        op_kwargs={"script_relpath": script_relpath},
        **kwargs,
    )


# Por entidade: ingest (API) → bronze→silver → silver→gold → dim, + arquivamentos.
ENTITIES = {
    "camp": {
        "ingest": "1_bronze/cargaDiaria/1_diaAdia_face_camp.py",
        "b2s": "1_bronze/transformers/1_facebook_campaign_bronze_to_silver_parquet.py",
        "move_b": "1_bronze/transformers/2_facebook_campaign_moveTOcarregados.py",
        "s2g": "2_silver/1_facebook_campaing_silver_TO_gold.py",
        "move_s": "2_silver/2_facebook_campaing_moveTOcarregados.py",
        "dim": "3_gold/1_dim_camp.py",
    },
    "adset": {
        "ingest": "1_bronze/cargaDiaria/2_diaAdia_face_adset.py",
        "b2s": "1_bronze/transformers/3_facebook_adset_bronze_to_silver_parquet.py",
        "move_b": "1_bronze/transformers/4_facebook_adset_moveTocarregados.py",
        "s2g": "2_silver/3_facebook_adset_silver_to_gold.py",
        "move_s": "2_silver/4_facebook_adset_moveTOcarregados.py",
        "dim": "3_gold/2_dim_adset.py",
    },
    "ad": {
        "ingest": "1_bronze/cargaDiaria/3_diaAdia_face_ad.py",
        "b2s": "1_bronze/transformers/5_facebook_ad_bronze_to_silver_parquet.py",
        "move_b": "1_bronze/transformers/6_facebook_ad_moveTOcarregados.py",
        "s2g": "2_silver/5_facebook_ad_silver_to_gold.py",
        "move_s": "2_silver/6_facebook_ad_moveTOcarregados.py",
        "dim": "3_gold/3_dim_ad.py",
    },
}


with DAG(
    dag_id="facebook_datamart",
    description="Data mart Facebook: API → bronze → silver → gold (Azure Blob + Polars)",
    schedule="0 12 * * 1-5",  # Seg-Sex meio-dia (BRT)
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,  # tasks movem/deletam blobs: 2 runs simultâneos corromperiam
    default_args={"retries": 1},
    tags=["facebook", "medallion", "azure-blob"],
) as dag:

    pipeline_done = EmptyOperator(task_id="pipeline_done")

    for ent, s in ENTITIES.items():
        ingest = fb_task(f"{ent}_ingest_api", s["ingest"])
        bronze_to_silver = fb_task(f"{ent}_bronze_to_silver", s["b2s"])
        # arquivamentos fazem download/copy→delete: NÃO idempotentes → sem retry
        archive_bronze = fb_task(f"{ent}_archive_bronze", s["move_b"], retries=0)
        silver_to_gold = fb_task(f"{ent}_silver_to_gold", s["s2g"])
        archive_silver = fb_task(f"{ent}_archive_silver", s["move_s"], retries=0)
        dim = fb_task(f"{ent}_dim", s["dim"])

        ingest >> bronze_to_silver
        bronze_to_silver >> archive_bronze          # arquiva o JSON do bronze
        bronze_to_silver >> silver_to_gold
        silver_to_gold >> archive_silver            # arquiva o parquet do silver
        silver_to_gold >> dim
        [archive_bronze, archive_silver, dim] >> pipeline_done
