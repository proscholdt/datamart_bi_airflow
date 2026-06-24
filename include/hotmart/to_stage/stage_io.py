"""STAGE do hotmart — grava o JSON da API no container de stage.

    datamartbistage/source_hotmart/<src>/<stem>_<name>.json

Promoção stage→raw e limpeza em to_raw/raw_io.py. As pastas source_hotmart/<src>
são permanentes — só os arquivos são removidos na limpeza.
"""
import json

from azure.storage.blob import ContentSettings

from hotmart_config import STAGE, get_blob_service_client
from hotmart_meta import STAGE_DIR, STEM


def write_stage(src, data_list, name):
    """Grava UM arquivo no STAGE: <stage_dir>/<stem>_<name>.json."""
    path = f"{STAGE_DIR[src]}/{STEM[src]}_{name}.json"
    blob = get_blob_service_client().get_blob_client(container=STAGE, blob=path)
    blob.upload_blob(
        json.dumps(data_list, ensure_ascii=False, indent=2).encode("utf-8"),
        overwrite=True,
        content_settings=ContentSettings(content_type="application/json"),
    )
    print(f"📥 stage: {STAGE}/{path} ({len(data_list)} registros)")
