
import os
import json
from io import BytesIO
import polars as pl
from facebook_config import get_container_client

# ================================
# Variáveis de ambiente / pastas
# ================================
BRONZE_FOLDER = "source_facebook/facebook_adset"
SILVER_FOLDER = "source_facebook/facebook_adset"

# ================================
# Conexão Azure Blob
# ================================
print("☁️  Conectando ao Azure Blob Storage...")
bronze_client = get_container_client("bronze")
silver_client = get_container_client("silver")

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
