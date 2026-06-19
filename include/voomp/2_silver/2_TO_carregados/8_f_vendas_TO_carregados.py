import os
import io

from voomp_config import get_container_client

container_client = get_container_client("silver")

# Caminhos
pasta_origem = 'source_voomp/f_vendas'
pasta_destino = 'source_voomp/f_vendas_carregados'

# Listar blobs na pasta origem
blobs = container_client.list_blobs(name_starts_with=pasta_origem + '/')

for blob in blobs:
    print(f"🔄 Movendo: {blob.name}")

    # Baixar o blob da origem
    downloader = container_client.download_blob(blob)
    blob_data = io.BytesIO()
    downloader.readinto(blob_data)
    blob_data.seek(0)

    # Definir o novo caminho na pasta destino
    nome_arquivo = os.path.basename(blob.name)
    novo_blob_name = f"{pasta_destino}/{nome_arquivo}"

    # Upload para a pasta destino
    container_client.upload_blob(name=novo_blob_name, data=blob_data, overwrite=True)
    print(f"✅ Blob movido para: {novo_blob_name}")

    # Opcional: deletar o blob original após mover
    container_client.delete_blob(blob)
    print(f"🗑️ Blob original excluído: {blob.name}")
