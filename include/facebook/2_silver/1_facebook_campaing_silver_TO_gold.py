

import os
from io import BytesIO
import polars as pl
from facebook_config import get_container_client

# ================================
# Variáveis de ambiente / pastas
# ================================
SILVER_FOLDER = "source_facebook/facebook_camp"
GOLD_FOLDER = "source_facebook/facebook_camp"

# ================================
# Conexão Azure Blob
# ================================
print("☁️  Conectando ao Azure Blob Storage...")
silver_client = get_container_client("silver")
gold_client = get_container_client("gold")

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
