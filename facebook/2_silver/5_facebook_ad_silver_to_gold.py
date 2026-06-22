
import os
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
CONTAINER_SILVER = "silver"
CONTAINER_GOLD = "gold"
SILVER_FOLDER = "source_facebook/facebook_ad"
GOLD_FOLDER = "source_facebook/facebook_ad"

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
silver_client = blob_service_client.get_container_client(CONTAINER_SILVER)
gold_client = blob_service_client.get_container_client(CONTAINER_GOLD)

# ================================
# Processar arquivos Parquet da Silver para a Gold
# ================================
print("🔄 Iniciando migração de arquivos Parquet da Silver para Gold...")

prefix = f"{SILVER_FOLDER}/"
blobs = silver_client.list_blobs(name_starts_with=prefix)

for blob in blobs:
    if not blob.name.startswith(prefix) or "/" in blob.name[len(prefix):]:
        continue
    if not blob.name.endswith(".parquet"):
        continue

    print(f"📄 Lendo {blob.name}...")
    blob_data = silver_client.download_blob(blob.name).readall()

    # Leitura do Parquet como Polars
    try:
        df = pl.read_parquet(BytesIO(blob_data))
    except Exception as e:
        print(f"❌ Erro ao ler {blob.name}: {e}")
        continue

    # Nome de destino na Gold
    filename = os.path.basename(blob.name)
    gold_path = f"{GOLD_FOLDER}/{filename}"

    # Salvar em buffer
    buffer = BytesIO()
    df.write_parquet(buffer)
    buffer.seek(0)

    # Upload para Gold
    print(f"⬆️  Enviando {gold_path} para Gold...")
    gold_client.upload_blob(gold_path, buffer, overwrite=True)

print("✅ Migração concluída.")


