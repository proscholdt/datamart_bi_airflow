


import os
import hashlib
import io
import polars as pl

from voomp_config import get_container_client, BRONZE as CONTAINER_BRONZE, SILVER as CONTAINER_SILVER

bronze_client = get_container_client("bronze")
silver_client = get_container_client("silver")

# Função para gerar hash
def gerar_hash(texto):
    if texto is None or texto != texto:
        texto = ''
    return hashlib.md5(str(texto).encode('utf-8')).hexdigest()

# Caminhos
pasta_origem = 'source_voomp/voomp'
pasta_destino_vendas = 'source_voomp/f_vendas'

# Listar blobs na pasta origem, evitando arquivos que começam com "+"
blobs = bronze_client.list_blobs(name_starts_with=pasta_origem + '/')
blobs = [b for b in blobs if b.name.lower().endswith('.xlsx') and not os.path.basename(b.name).startswith('+')]

for blob in blobs:
    print(f"🔍 Processando: {blob.name}")

    nome_base = os.path.splitext(os.path.basename(blob.name))[0]

    # Baixar o blob
    downloader = bronze_client.download_blob(blob)
    blob_data = io.BytesIO()
    downloader.readinto(blob_data)
    blob_data.seek(0)
    
    for sheet_name in ["Exportação de vendas"]:
        try:
            pl_df = pl.read_excel(blob_data, sheet_name=sheet_name)
            print(f"  ➡️ Aba: {sheet_name}")

            # Filtrar apenas linhas com Nome do comprador e Email do comprador não nulos
            pl_df = pl_df.filter(
                pl_df['Nome do comprador'].is_not_null() & pl_df['Email do comprador'].is_not_null()
            )

            # Forçar as colunas a serem string (garante compatibilidade)
            pl_df = pl_df.with_columns([
                pl_df['Nome do comprador'].cast(pl.Utf8),
                pl_df['Email do comprador'].cast(pl.Utf8),
                pl_df['Nome Afiliado'].cast(pl.Utf8)
            ])

            # Adicionar IDs com return_dtype e skip_nulls=False
            pl_df = pl_df.with_columns([
                (pl_df['Nome do comprador'].fill_null('') + pl_df['Email do comprador'].fill_null(''))
                .map_elements(gerar_hash, return_dtype=pl.Utf8, skip_nulls=False).alias('ID_Cliente'),

                pl_df['Nome Afiliado'].fill_null('')
                .map_elements(gerar_hash, return_dtype=pl.Utf8, skip_nulls=False).alias('ID_Afiliado')
            ])

            # ✅ Remover linhas onde ID Venda é 445793 ou 445784
            pl_df = pl_df.filter(~pl_df['ID Venda'].is_in([445793, 445784]))

            # Criar nome do arquivo: nome do arquivo original + "_" + nome da aba (sem espaços)
            nome_arquivo = f"{nome_base}_{sheet_name.replace(' ', '_')}.parquet"

            # Pasta destino (apenas vendas)
            destino_blob = f"{pasta_destino_vendas}/{nome_arquivo}"

            # Converter para bytes
            parquet_buffer = io.BytesIO()
            pl_df.write_parquet(parquet_buffer)
            parquet_buffer.seek(0)

            # Enviar para o container Silver
            silver_client.upload_blob(name=destino_blob, data=parquet_buffer, overwrite=True)

            print(f"✅ Arquivo salvo na Silver: {CONTAINER_SILVER}/{destino_blob}")

            blob_data.seek(0)  # necessário para ler novamente

        except Exception as e:
            print(f"🚫 Erro ao processar a aba {sheet_name}: {e}")
            raise
