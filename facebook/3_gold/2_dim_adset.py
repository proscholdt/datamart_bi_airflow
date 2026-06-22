import os
import polars as pl
from io import BytesIO
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# Carregar variáveis de ambiente
load_dotenv()

# Configurações Azure
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = "gold"
SOURCE_FOLDER = "source_facebook/facebook_adset"
DEST_FOLDER = "source_facebook/dim_adset"
OUTPUT_FILE_NAME = "dim_adset.parquet"

# Conectar ao Blob Storage
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# Listar arquivos Parquet da pasta de origem
blobs = container_client.list_blobs(name_starts_with=SOURCE_FOLDER)
arquivos_parquet = [b.name for b in blobs if b.name.endswith(".parquet")]

# Carregar dados relevantes de cada arquivo
tabelas = []
for blob_name in arquivos_parquet:
    try:
        print(f"📥 Lendo {blob_name}")
        blob_data = container_client.download_blob(blob_name).readall()
        df = pl.read_parquet(BytesIO(blob_data)).select(["adset_id", "adset_name"])
        tabelas.append(df)
    except Exception as e:
        print(f"⚠️ Erro ao processar {blob_name}: {e}")

# Concatenar e remover duplicados
if tabelas:
    df_total = pl.concat(tabelas).unique()
    
    # Salvar buffer e fazer upload
    buffer = BytesIO()
    df_total.write_parquet(buffer)
    buffer.seek(0)

    destino_blob = f"{DEST_FOLDER}/{OUTPUT_FILE_NAME}"
    container_client.upload_blob(destino_blob, buffer, overwrite=True)
    print(f"✅ Dimensão salva em: {destino_blob}")
else:
    print("🚫 Nenhum arquivo válido encontrado.")
