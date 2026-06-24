"""STAGE do facebook — escrita da extração da API no container de stage.

O ingest (cargaDiaria) escreve o JSON cru de cada dia aqui, no formato:
    datamartbistage/source_facebook/facebook_<entity>/<stem>_<dia>.json
A promoção stage→raw e a limpeza ficam em to_raw/raw_io.py (stage_to_raw).
As pastas (source_facebook/facebook_<entity>) são permanentes — só os arquivos
são removidos na limpeza.
"""
import json

from azure.storage.blob import ContentSettings

from facebook_config import STAGE, get_blob_service_client
from fb_meta import ENTITIES


def stage_dir(entity):
    return ENTITIES[entity]["raw_dir"]  # source_facebook/facebook_<entity>


def write_stage(entity, data_list, event_date):
    """Grava o JSON cru de UM dia no STAGE: <stage_dir>/<stem>_<dia>.json."""
    cfg = ENTITIES[entity]
    path = f"{cfg['raw_dir']}/{cfg['stem']}_{event_date}.json"
    blob = get_blob_service_client().get_blob_client(container=STAGE, blob=path)
    blob.upload_blob(
        json.dumps(data_list, ensure_ascii=False, indent=2).encode("utf-8"),
        overwrite=True,
        content_settings=ContentSettings(content_type="application/json"),
    )
    print(f"📥 stage: {STAGE}/{path} ({len(data_list)} registros)")
