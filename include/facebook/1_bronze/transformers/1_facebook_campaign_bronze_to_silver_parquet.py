# ==========================================================
# Transforma campanhas do Facebook da Bronze para Silver (PARQUET)
# ==========================================================

# import os
# import sys
# from datetime import datetime
# from dotenv import load_dotenv
# from azure.storage.blob import BlobServiceClient
# from io import BytesIO
# import polars as pl
# import json

# # Caminho raiz do projeto
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

# # Carrega variáveis de ambiente
# print("⚙️  Carregando variáveis de ambiente...")
# load_dotenv()

# # Azure
# STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
# STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
# CONTAINER_BRONZE = "bronze"
# CONTAINER_SILVER = "silver"
# BRONZE_FOLDER = "source_facebook/facebook_camp"
# SILVER_FOLDER = "source_facebook/facebook_camp"

# # Conexão Azure
# print("☁️  Conectando ao Azure Blob Storage...")
# connection_string = (
#     f"DefaultEndpointsProtocol=https;"
#     f"AccountName={STORAGE_ACCOUNT_NAME};"
#     f"AccountKey={STORAGE_ACCOUNT_KEY};"
#     f"EndpointSuffix=core.windows.net"
# )
# blob_service_client = BlobServiceClient.from_connection_string(connection_string)
# bronze_client = blob_service_client.get_container_client(CONTAINER_BRONZE)
# silver_client = blob_service_client.get_container_client(CONTAINER_SILVER)

# # Buscar arquivos da pasta correta
# print(f"🔎 Buscando arquivos em {BRONZE_FOLDER}...")
# blobs = bronze_client.list_blobs(name_starts_with=BRONZE_FOLDER)
# json_blobs = [
#     blob.name for blob in blobs
#     if blob.name.startswith(f"{BRONZE_FOLDER}/") and blob.name.endswith(".json")
# ]

# if not json_blobs:
#     print("⚠️ Nenhum arquivo encontrado.")
#     exit()

# for blob_name in json_blobs:
#     print(f"\n📥 Processando: {blob_name}")
#     blob_data = bronze_client.download_blob(blob_name).readall()
#     json_data = json.loads(blob_data)

#     # Pega lista de campanhas
#     campaigns = json_data["data"] if isinstance(json_data, dict) and "data" in json_data else json_data

#     if not campaigns:
#         print("⚠️ Nenhum dado encontrado. Pulando arquivo.")
#         continue

#     df_raw = pl.DataFrame(campaigns)

#     # Explodir insights (se existir)
#     if "insights" in df_raw.columns:
#         print("🔄 Explodindo campo 'insights'...")
#         df = df_raw.explode("insights")
#         df = df.with_columns([
#             pl.col("insights").struct.rename_fields([
#                 "date_start", "date_stop", "spend", "impressions", "clicks", "ctr", "cpc", "reach",
#                 "frequency", "cost_per_unique_click", "quality_ranking",
#                 "engagement_rate_ranking", "leads", "purchases", "purchase_value"
#             ]).alias("insights")
#         ])
#         df = df.unnest("insights")
#     else:
#         print("⚠️ Campo 'insights' não encontrado.")
#         df = df_raw

#     # Nome final do arquivo
#     now = datetime.now().strftime("%Y%m%d_%H%M%S")
#     file_name = os.path.basename(blob_name).replace(".json", f"_silver_{now}.parquet")
#     silver_path = f"{SILVER_FOLDER}/{file_name}"

#     # Salvar Parquet
#     print("💾 Salvando como Parquet...")
#     buffer = BytesIO()
#     df.write_parquet(buffer)
#     buffer.seek(0)

#     # Upload
#     print(f"🚀 Enviando para Silver: {silver_path}")
#     silver_client.upload_blob(name=silver_path, data=buffer, overwrite=True)

#     print("✅ Arquivo salvo com sucesso na Silver.")


import os
import json
from io import BytesIO
import polars as pl
from facebook_config import get_container_client

# ================================
# Variáveis de ambiente / pastas
# ================================
BRONZE_FOLDER = "source_facebook/facebook_camp"
SILVER_FOLDER = "source_facebook/facebook_camp"

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
