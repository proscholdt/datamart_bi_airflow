# # ==========================================================
# # Move arquivos de facebook_camp para facebook_camp_carregados no Azure (bronze)
# # ==========================================================

# import os
# from dotenv import load_dotenv
# from azure.storage.blob import BlobServiceClient

# # Carregar variáveis de ambiente
# load_dotenv()
# STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
# STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
# CONTAINER_NAME = "bronze"
# FOLDER_ORIGEM = "source_facebook/facebook_camp"
# FOLDER_DESTINO = "source_facebook/facebook_camp_carregados"

# # Conexão com Azure
# connection_string = (
#     f"DefaultEndpointsProtocol=https;"
#     f"AccountName={STORAGE_ACCOUNT_NAME};"
#     f"AccountKey={STORAGE_ACCOUNT_KEY};"
#     f"EndpointSuffix=core.windows.net"
# )
# blob_service = BlobServiceClient.from_connection_string(connection_string)
# container_client = blob_service.get_container_client(CONTAINER_NAME)

# # Listar blobs
# print(f"📦 Buscando arquivos em: {FOLDER_ORIGEM}")
# blobs = container_client.list_blobs(name_starts_with=FOLDER_ORIGEM)

# for blob in blobs:
#     blob_name = blob.name
#     file_name = os.path.basename(blob_name)

#     if not file_name or not blob_name.endswith(".json"):
#         continue

#     destino_blob = f"{FOLDER_DESTINO}/{file_name}"
#     print(f"🔁 Movendo {blob_name} → {destino_blob}")

#     # Criar blob destino e iniciar cópia
#     dest_blob_client = container_client.get_blob_client(destino_blob)
#     src_url = f"{container_client.url}/{blob_name}"

#     dest_blob_client.start_copy_from_url(src_url)
#     container_client.delete_blob(blob_name)

#     print(f"✅ Arquivo movido: {file_name}")



# ==========================================================
# Move arquivos de facebook_camp para facebook_camp_carregados no Azure (bronze)
# ==========================================================

import os
import time
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, BlobProperties

# Carregar variáveis de ambiente
load_dotenv()
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = "bronze"
FOLDER_ORIGEM = "source_facebook/facebook_camp/"  # ← Barra final para filtrar corretamente
FOLDER_DESTINO = "source_facebook/facebook_camp_carregados"

# Conexão com Azure
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_service = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service.get_container_client(CONTAINER_NAME)

# Listar arquivos da pasta de origem
print(f"📦 Buscando arquivos em: {FOLDER_ORIGEM}")
blobs = container_client.list_blobs(name_starts_with=FOLDER_ORIGEM)

for blob in blobs:
    blob_name = blob.name
    file_name = os.path.basename(blob_name)

    # ⚠️ Ignorar subpastas ou arquivos fora da pasta base
    if "_carregados" in blob_name or not blob_name.endswith(".json"):
        continue

    destino_blob = f"{FOLDER_DESTINO}/{file_name}"
    print(f"🔁 Movendo {blob_name} → {destino_blob}")

    # Criar blob destino
    dest_blob_client = container_client.get_blob_client(destino_blob)
    src_url = f"{container_client.url}/{blob_name}"

    # Verifica se já existe no destino (opcional: pode deletar se quiser sobrescrever)
    if dest_blob_client.exists():
        print(f"⚠️ Arquivo já existe no destino. Ignorando: {file_name}")
        continue

    # Iniciar cópia
    copy = dest_blob_client.start_copy_from_url(src_url)

    # Aguardar cópia completar
    for _ in range(10):
        props: BlobProperties = dest_blob_client.get_blob_properties()
        if props.copy.status == "success":
            break
        elif props.copy.status == "pending":
            time.sleep(1)
        else:
            print(f"❌ Falha ao copiar {file_name}. Status: {props.copy.status}")
            continue

    # Deletar arquivo original após cópia
    container_client.delete_blob(blob_name)
    print(f"✅ Arquivo movido: {file_name}")
