import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta

# Carregar variáveis de ambiente
load_dotenv()

# Configurações
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = "silver"
SOURCE_FOLDER = "source_facebook/facebook_adset"
DEST_FOLDER = "source_facebook/facebook_adset_carregados"

# Conexão Azure Blob
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# Listar arquivos EXATAMENTE na pasta de origem
print(f"🔍 Listando arquivos em '{SOURCE_FOLDER}/'...")

blobs = []
for b in container_client.list_blobs(name_starts_with=SOURCE_FOLDER + "/"):
    # Evitar pegar arquivos de subpastas ou com nomes parecidos
    if not b.name.startswith(SOURCE_FOLDER + "/"):
        continue
    relative_path = b.name[len(SOURCE_FOLDER) + 1:]  # remove o prefixo da pasta e a barra
    if "/" in relative_path:
        print(f"⚠️ Ignorando subpasta: {b.name}")
        continue
    if not b.name.endswith(".parquet"):
        print(f"⚠️ Ignorando arquivo que não é parquet: {b.name}")
        continue
    blobs.append(b.name)

if not blobs:
    print("⚠️ Nenhum arquivo encontrado para mover.")
    exit()

# Mover e apagar
for blob_name in blobs:
    file_name = blob_name.split("/")[-1]
    dest_blob_name = f"{DEST_FOLDER}/{file_name}"

    # Gerar SAS de leitura para copiar
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
    print(f"📤 Copiando: {file_name}")
    dest_blob = container_client.get_blob_client(dest_blob_name)
    dest_blob.start_copy_from_url(source_url)

    # Remover original
    print(f"🗑️ Deletando original: {file_name}")
    container_client.delete_blob(blob_name)

print("✅ Todos os arquivos foram movidos e removidos com sucesso.")
