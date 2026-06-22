

import os
import json
from io import BytesIO
from azure.storage.blob import BlobServiceClient
import polars as pl
from dotenv import load_dotenv

# ================================
# Variáveis de ambiente
# ================================
print("⚙️  Carregando variáveis de ambiente...")
load_dotenv()
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_BRONZE = "bronze"
CONTAINER_SILVER = "silver"
BRONZE_FOLDER = "source_facebook/facebook_ad"
SILVER_FOLDER = "source_facebook/facebook_ad"

# ================================
# Conexão Azure Blob
# ================================
print("☁️  Conectando ao Azure Blob Storage...")
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
bronze_client = blob_service_client.get_container_client(CONTAINER_BRONZE)
silver_client = blob_service_client.get_container_client(CONTAINER_SILVER)

# ================================
# Processar arquivos JSON para Parquet
# ================================
print("🔄 Iniciando conversão de arquivos JSON da Bronze para Parquet na Silver...")

# Garante que só arquivos da pasta EXATA "facebook_camp/" sejam lidos
prefix = f"{BRONZE_FOLDER}/"
blobs = bronze_client.list_blobs(name_starts_with=prefix)

for blob in blobs:
    # Garante que não sejam subpastas ou arquivos de outras pastas
    if not blob.name.startswith(prefix) or "/" in blob.name[len(prefix):]:
        continue
    if not blob.name.endswith(".json"):
        continue

    print(f"📄 Lendo {blob.name}...")
    blob_data = bronze_client.download_blob(blob.name).readall()
    json_data = json.loads(blob_data)

    if not json_data:
        print(f"⚠️  Arquivo vazio: {blob.name}")
        continue

    # Converter para DataFrame Polars
    try:
        df = pl.DataFrame(json_data)
    except Exception as e:
        print(f"❌ Erro ao converter {blob.name} para DataFrame: {e}")
        continue

    # Nome do novo arquivo
    filename = os.path.basename(blob.name).replace(".json", ".parquet")
    parquet_path = f"{SILVER_FOLDER}/{filename}"

    # Salvar em buffer
    buffer = BytesIO()
    df.write_parquet(buffer)
    buffer.seek(0)

    # Upload na Silver
    print(f"⬆️  Enviando {parquet_path} para Silver...")
    silver_client.upload_blob(parquet_path, buffer, overwrite=True)

print("✅ Conversão concluída.")