# import os
# import io
# import polars as pl
# from dotenv import load_dotenv
# from azure.storage.blob import BlobServiceClient

# # Carregar variáveis de ambiente
# load_dotenv()

# # Configurações Azure
# STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
# STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
# CONTAINER_SILVER = "silver"
# CONTAINER_GOLD = "gold"

# # Conexão Azure Blob
# connection_string = (
#     f"DefaultEndpointsProtocol=https;"
#     f"AccountName={STORAGE_ACCOUNT_NAME};"
#     f"AccountKey={STORAGE_ACCOUNT_KEY};"
#     f"EndpointSuffix=core.windows.net"
# )

# blob_service_client = BlobServiceClient.from_connection_string(connection_string)
# silver_client = blob_service_client.get_container_client(CONTAINER_SILVER)
# gold_client = blob_service_client.get_container_client(CONTAINER_GOLD)

# # Caminhos
# pasta_f_vendas = 'source_voomp/f_vendas'
# pasta_f_vendas_gold = 'source_voomp/f_vendas'
# nome_f_vendas_gold = 'f_vendas.parquet'

# # Listar arquivos na pasta f_vendas da Silver
# blobs = silver_client.list_blobs(name_starts_with=pasta_f_vendas + '/')
# blobs = [b for b in blobs if b.name.lower().endswith('.parquet')]

# if len(blobs) != 1:
#     raise Exception(f"❌ Esperado 1 arquivo, mas encontrei {len(blobs)} arquivos.")

# blob = blobs[0]

# print(f"🔍 Lendo: {blob.name}")

# downloader = silver_client.download_blob(blob)
# blob_data = io.BytesIO()
# downloader.readinto(blob_data)
# blob_data.seek(0)

# pl_df = pl.read_parquet(blob_data)

# # ✅ Aqui você define as colunas que quer levar para a Gold
# pl_df = pl_df.select([
#     'ID Venda',
#     'Data da venda',
#     'Data de pagamento',
#     'Data de vencimento do boleto',
#     'Data liberação do saldo',
#     'ID Produto',
#     'ID Oferta',
#     'Método de pagamento',
#     'Forma de pagamento',
#     'Cupom',
#     'Valor Oferta',
#     'Valor Pago',
# 	'Taxa Voomp',
# 	'Valor comissão afiliado',
#     'Valor comissão co-produtor',
# 	'Valor Recebido',
# 	'Taxa de parcelamento',
# 	'Status da venda',
# 	'Motivo do reembolso',
# 	'Motivo da recusa',
# 	'Venda inteligente',
#     'Tipo de cobrança',
# 	'ID Contrato',
#     'Status de Contrato',
#     'Período',
#     'Recorrência atual',
#     'Recorrência total',
#     'Assinaturas em atraso (dias)',
#     'Nota fiscal',
#     'Order Bump',
#     'UF Origem',
#     'Taxa de câmbio',
#     'Link Boleto',
#     'Cod. de Barras',
#     'ID_Afiliado',
#     'ID_Cliente'
# ])

# # Salvar como Parquet
# parquet_buffer = io.BytesIO()
# pl_df.write_parquet(parquet_buffer)
# parquet_buffer.seek(0)

# # Upload para a Gold
# destino_blob = f"{pasta_f_vendas_gold}/{nome_f_vendas_gold}"
# gold_client.upload_blob(name=destino_blob, data=parquet_buffer, overwrite=True)

# print(f"✅ Arquivo f_vendas salvo na Gold: {CONTAINER_GOLD}/{destino_blob}")



import os
import io
import polars as pl

from voomp_config import get_container_client, SILVER as CONTAINER_SILVER, GOLD as CONTAINER_GOLD

silver_client = get_container_client("silver")
gold_client = get_container_client("gold")

# Caminhos
pasta_f_vendas = 'source_voomp/f_vendas'
pasta_f_vendas_gold = 'source_voomp/f_vendas'
nome_f_vendas_gold = 'f_vendas.parquet'

# Listar arquivos na pasta f_vendas da Silver
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

# ✅ Seleção das colunas que quer levar para a Gold
pl_df = pl_df.select([
    'ID Venda',
    'Data da venda',
    'Data de pagamento',
    'Data de vencimento do boleto',
    'Data liberação do saldo',
    'ID Produto',
    'ID Oferta',
    'Método de pagamento',
    'Forma de pagamento',
    'Cupom',
    'Valor Oferta',
    'Valor Pago',
    'Taxa Voomp',
    'Valor comissão afiliado',
    'Valor comissão co-produtor',
    'Valor Recebido',
    'Status da venda',
    'Motivo do reembolso',
    'Motivo da recusa',
    'Venda inteligente',
    'Tipo de cobrança',
    'ID Contrato',
    'Status de Contrato',
    'Período',
    'Recorrência atual',
    'Recorrência total',
    'Assinaturas em atraso (dias)',
    'Nota fiscal',
    'Order Bump',
    'UF Origem',
    'Taxa de câmbio',
    'Link Boleto',
    'Cod. de Barras',
    'ID_Afiliado',
    'ID_Cliente'
])

# Salvar como Parquet
parquet_buffer = io.BytesIO()
pl_df.write_parquet(parquet_buffer)
parquet_buffer.seek(0)

# Upload para a Gold
destino_blob = f"{pasta_f_vendas_gold}/{nome_f_vendas_gold}"
gold_client.upload_blob(name=destino_blob, data=parquet_buffer, overwrite=True)

print(f"✅ Arquivo f_vendas salvo na Gold: {CONTAINER_GOLD}/{destino_blob}")
