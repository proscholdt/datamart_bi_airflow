import os
import polars as pl
from io import BytesIO
from datetime import datetime
from os.path import dirname
from facebook_config import get_container_client

# Pastas de origem/destino
FOLDER_ORIGEM = "source_facebook/facebook_camp"
FOLDER_DESTINO = "source_facebook/dim_camp"

# Conexão com Azure Blob Storage
container_client = get_container_client("gold")

# Buscar arquivos diretamente da pasta correta
print(f"🔍 Buscando arquivos em: {FOLDER_ORIGEM}")
blobs = [
    b.name for b in container_client.list_blobs()
    if dirname(b.name) == FOLDER_ORIGEM and b.name.endswith(".parquet")
]

if not blobs:
    print("⚠️ Nenhum arquivo Parquet encontrado.")
    exit()

# Colunas obrigatórias
required_columns = ["campaign_id", "campaign_name", "spend"]

# Unificação
dataframes = []
for blob_name in blobs:
    print(f"\n📥 Lendo: {blob_name}")
    blob_data = container_client.download_blob(blob_name).readall()
    try:
        df = pl.read_parquet(BytesIO(blob_data))
    except Exception as e:
        print(f"❌ Erro ao ler {blob_name}: {e}")
        continue

    # Renomear se necessário
    renomear = {}
    if "id" in df.columns and "campaign_id" not in df.columns:
        renomear["id"] = "campaign_id"
    if "name" in df.columns and "campaign_name" not in df.columns:
        renomear["name"] = "campaign_name"
    if renomear:
        print(f"♻️ Renomeando colunas: {renomear}")
        df = df.rename(renomear)

    if not all(col in df.columns for col in required_columns):
        print(f"⚠️ Ignorado: colunas ausentes → {blob_name}")
        continue

    df = df.select([col for col in required_columns if col in df.columns])
    dataframes.append(df)

if not dataframes:
    print("⚠️ Nenhum DataFrame válido para unificar.")
    exit()

# Concatenar todos
df_final = pl.concat(dataframes, how="vertical")

# ✅ Eliminar duplicados pelo campaign_id
df_final = df_final.unique(subset=["campaign_id"])

# Nome do arquivo final
file_name = f"{FOLDER_DESTINO}/dim_camp.parquet"

# Salvar consolidado
print("💾 Gerando arquivo final...")
buffer = BytesIO()
df_final.write_parquet(buffer)
buffer.seek(0)

print(f"🚀 Enviando para: {file_name}")
container_client.upload_blob(name=file_name, data=buffer, overwrite=True)

print("✅ Arquivo consolidado salvo com sucesso na pasta 'dim_camp'.")
