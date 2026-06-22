import os
from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta
from facebook_config import get_container_client, SILVER as CONTAINER_NAME

STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
SOURCE_FOLDER = "source_facebook/facebook_camp"
DEST_FOLDER = "source_facebook/facebook_camp_carregados"

# Conexão com o Azure Blob
container_client = get_container_client("silver")

# Listar todos os blobs e filtrar os que estão exatamente na pasta SOURCE_FOLDER
blobs = [
    b.name for b in container_client.list_blobs()
    if b.name.endswith(".parquet") and os.path.dirname(b.name) == SOURCE_FOLDER
]

if not blobs:
    print("⚠️ Nenhum arquivo encontrado para mover.")
    exit()

print(f"🔁 Movendo {len(blobs)} arquivos da pasta '{SOURCE_FOLDER}' para '{DEST_FOLDER}'...")

for blob_name in blobs:
    filename = os.path.basename(blob_name)
    dest_blob_name = f"{DEST_FOLDER}/{filename}"

    # Criar SAS token temporário para leitura
    sas_token = generate_blob_sas(
        account_name=STORAGE_ACCOUNT_NAME,
        container_name=CONTAINER_NAME,
        blob_name=blob_name,
        account_key=STORAGE_ACCOUNT_KEY,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(hours=1)
    )
    source_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{CONTAINER_NAME}/{blob_name}?{sas_token}"

    # Copiar para destino
    dest_blob = container_client.get_blob_client(dest_blob_name)
    dest_blob.start_copy_from_url(source_url)
    print(f"📤 Copiado: {filename}")

    # Remover original
    container_client.delete_blob(blob_name)
    print(f"🗑️ Deletado original: {filename}")

print("✅ Todos os arquivos foram movidos com sucesso.")
