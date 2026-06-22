import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta

# Carregar variáveis de ambiente
load_dotenv()

# Configurações Azure
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = "silver"
SOURCE_FOLDER = "source_facebook/facebook_ad"
DEST_FOLDER = "source_facebook/facebook_ad_carregados"

# Conectar ao Blob Storage
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# Buscar arquivos exatos da pasta (sem subpastas)
print("🔍 Listando arquivos para mover...")
blobs = [
    b.name for b in container_client.list_blobs(name_starts_with=SOURCE_FOLDER + "/")
    if b.name.startswith(SOURCE_FOLDER + "/") and b.name.endswith(".parquet") and "/" not in b.name[len(SOURCE_FOLDER)+1:]
]

if not blobs:
    print("⚠️ Nenhum arquivo encontrado em facebook_ad.")
    exit()

# Mover arquivos
for blob_name in blobs:
    file_name = blob_name.split("/")[-1]
    dest_blob_name = f"{DEST_FOLDER}/{file_name}"

    # Criar SAS para copiar
    sas_token = generate_blob_sas(
        account_name=STORAGE_ACCOUNT_NAME,
        container_name=CONTAINER_NAME,
        blob_name=blob_name,
        account_key=STORAGE_ACCOUNT_KEY,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(hours=1)
    )
    source_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{CONTAINER_NAME}/{blob_name}?{sas_token}"

    # Copiar
    print(f"📤 Copiando: {file_name}")
    dest_blob = container_client.get_blob_client(dest_blob_name)
    dest_blob.start_copy_from_url(source_url)

    # Deletar original
    print(f"🗑️ Deletando original: {file_name}")
    container_client.delete_blob(blob_name)

print("✅ Todos os arquivos foram movidos com sucesso.")
