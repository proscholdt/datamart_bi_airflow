import os
import hashlib
import io
import polars as pl

from voomp_config import get_container_client, BRONZE as CONTAINER_BRONZE, SILVER as CONTAINER_SILVER

bronze_client = get_container_client("bronze")
silver_client = get_container_client("silver")

# Caminhos
pasta_origem = 'source_voomp/projetadas_voomp'
pasta_destino = 'source_voomp/projetadas_voomp'

# Listar blobs na pasta origem
blobs = bronze_client.list_blobs(name_starts_with=pasta_origem + '/')

for blob in blobs:
    # Ignorar diretórios virtuais e arquivos sem extensão
    if not os.path.basename(blob.name):
        continue

    print(f"🔍 Processando: {blob.name}")

    nome_base, ext = os.path.splitext(os.path.basename(blob.name))

    # Apenas arquivos conhecidos (xlsx, csv)
    if ext.lower() not in ['.xlsx', '.xls', '.csv']:
        print(f"⚠️ Ignorando (extensão não suportada): {blob.name}")
        continue

    # Baixar o blob da Bronze
    downloader = bronze_client.download_blob(blob)
    blob_data = io.BytesIO()
    downloader.readinto(blob_data)
    blob_data.seek(0)

    try:
        # Ler o arquivo conforme a extensão
        if ext.lower() in ['.xlsx', '.xls']:
            # Lê a primeira aba por padrão
            pl_df = pl.read_excel(blob_data)
        else:  # .csv
            pl_df = pl.read_csv(blob_data)

        # Nome do arquivo Parquet de saída
        destino_blob = f"{pasta_destino}/{nome_base}.parquet"

        # Converter para Parquet em memória
        parquet_buffer = io.BytesIO()
        pl_df.write_parquet(parquet_buffer)
        parquet_buffer.seek(0)

        # Enviar para o container Silver
        silver_client.upload_blob(name=destino_blob, data=parquet_buffer, overwrite=True)

        print(f"✅ Arquivo salvo na Silver: {CONTAINER_SILVER}/{destino_blob}")

        # Deletar o arquivo de origem na Bronze após sucesso
        bronze_client.delete_blob(blob.name)
        print(f"🗑️ Arquivo deletado da Bronze: {CONTAINER_BRONZE}/{blob.name}")

    except Exception as e:
        print(f"🚫 Erro ao processar {blob.name}: {e}")
        raise
