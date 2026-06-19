import os
import io
import polars as pl

from voomp_config import get_container_client, SILVER as CONTAINER_SILVER, GOLD as CONTAINER_GOLD

silver_client = get_container_client("silver")
gold_client = get_container_client("gold")

# Caminhos
pasta_f_vendas = 'source_voomp/f_vendas'
pasta_dim_oferta = 'source_voomp/dim_oferta'
nome_dim_oferta = 'dim_oferta.parquet'

# Listar arquivos na pasta f_vendas
blobs = silver_client.list_blobs(name_starts_with=pasta_f_vendas + '/')
blobs = [b for b in blobs if b.name.lower().endswith('.parquet')]

if len(blobs) != 1:
    raise Exception(f"❌ Esperado 1 arquivo, mas encontrei {len(blobs)} arquivos.")

blob = blobs[0]

print(f"🔍 Lendo: {blob.name}")

downloader = silver_client.download_blob(blob)
blob_data = io.BytesIO()
downloader.readinto(blob_data)
blob_data.seek(0)

pl_df = pl.read_parquet(blob_data)

# Selecionar colunas desejadas
pl_df = pl_df.select([
    'ID Oferta',
    'Nome da oferta'
])

# ✅ Remover duplicatas
pl_df = pl_df.unique()

# Salvar como Parquet
parquet_buffer = io.BytesIO()
pl_df.write_parquet(parquet_buffer)
parquet_buffer.seek(0)

# Upload para a Gold
destino_blob = f"{pasta_dim_oferta}/{nome_dim_oferta}"
gold_client.upload_blob(name=destino_blob, data=parquet_buffer, overwrite=True)

print(f"✅ Arquivo dim_oferta salvo na Gold: {CONTAINER_GOLD}/{destino_blob}")
