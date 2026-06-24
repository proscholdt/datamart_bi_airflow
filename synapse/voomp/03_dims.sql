/* ============================================================================
   Synapse Serverless — 4 dimensões Delta da GOLD do voomp (pipeline novo).
     dim_afiliado / dim_cliente / dim_oferta / dim_produto
   Reaproveita data source [AzureDataLakeStore_DataMartBI_Gold] + [DeltaFormat]
   (IF NOT EXISTS abaixo só p/ rodar sozinho). Todas Delta reader v1 → Synapse lê.
   ============================================================================ */
USE biitvalleyschool;
GO

IF NOT EXISTS (SELECT 1 FROM sys.external_data_sources WHERE name = 'AzureDataLakeStore_DataMartBI_Gold')
    CREATE EXTERNAL DATA SOURCE [AzureDataLakeStore_DataMartBI_Gold]
    WITH (LOCATION = 'abfss://datamartbigold@saactivecampaign.dfs.core.windows.net/',
          CREDENTIAL = [Service_Principal_2025_2027]);
GO
IF NOT EXISTS (SELECT 1 FROM sys.external_file_formats WHERE name = 'DeltaFormat')
    CREATE EXTERNAL FILE FORMAT [DeltaFormat] WITH (FORMAT_TYPE = DELTA);
GO

/* ----------------------------- AFILIADO ---------------------------------- */
IF OBJECT_ID('gold.Voomp_Dim_Afiliado_airflow') IS NOT NULL
    DROP EXTERNAL TABLE [gold].[Voomp_Dim_Afiliado_airflow];
GO
CREATE EXTERNAL TABLE [gold].[Voomp_Dim_Afiliado_airflow]
(
    [ID_Afiliado]    VARCHAR(100),   -- MD5 hex (32)
    [Nome Afiliado]  VARCHAR(4000)
)
WITH (LOCATION = '/voomp/dim_afiliado/', DATA_SOURCE = [AzureDataLakeStore_DataMartBI_Gold], FILE_FORMAT = [DeltaFormat]);
GO

/* ------------------------------ CLIENTE ---------------------------------- */
IF OBJECT_ID('gold.Voomp_Dim_Cliente_airflow') IS NOT NULL
    DROP EXTERNAL TABLE [gold].[Voomp_Dim_Cliente_airflow];
GO
CREATE EXTERNAL TABLE [gold].[Voomp_Dim_Cliente_airflow]
(
    [Nome do comprador]   VARCHAR(4000),
    [Email do comprador]  VARCHAR(4000),
    [Número de telefone]  BIGINT,
    [Endereço físico]     VARCHAR(4000),
    [ID_Cliente]          VARCHAR(100),   -- MD5 hex (32)
    [CPF/CNPJ]            VARCHAR(20)
)
WITH (LOCATION = '/voomp/dim_cliente/', DATA_SOURCE = [AzureDataLakeStore_DataMartBI_Gold], FILE_FORMAT = [DeltaFormat]);
GO

/* ------------------------------- OFERTA ---------------------------------- */
IF OBJECT_ID('gold.Voomp_Dim_Oferta_airflow') IS NOT NULL
    DROP EXTERNAL TABLE [gold].[Voomp_Dim_Oferta_airflow];
GO
CREATE EXTERNAL TABLE [gold].[Voomp_Dim_Oferta_airflow]
(
    [ID Oferta]       VARCHAR(50),
    [Nome da oferta]  VARCHAR(4000)
)
WITH (LOCATION = '/voomp/dim_oferta/', DATA_SOURCE = [AzureDataLakeStore_DataMartBI_Gold], FILE_FORMAT = [DeltaFormat]);
GO

/* ------------------------------ PRODUTO ---------------------------------- */
IF OBJECT_ID('gold.Voomp_Dim_Produto_airflow') IS NOT NULL
    DROP EXTERNAL TABLE [gold].[Voomp_Dim_Produto_airflow];
GO
CREATE EXTERNAL TABLE [gold].[Voomp_Dim_Produto_airflow]
(
    [ID Produto]       BIGINT,
    [Nome do produto]  VARCHAR(4000),
    [Categoria]        VARCHAR(4000),
    [Tipo do produto]  VARCHAR(200)
)
WITH (LOCATION = '/voomp/dim_produto/', DATA_SOURCE = [AzureDataLakeStore_DataMartBI_Gold], FILE_FORMAT = [DeltaFormat]);
GO

/* ------------------------------- TESTES ---------------------------------- */
SELECT 'dim_afiliado' AS tabela, COUNT(*) AS linhas FROM [gold].[Voomp_Dim_Afiliado_airflow]  -- 4
UNION ALL SELECT 'dim_cliente', COUNT(*) FROM [gold].[Voomp_Dim_Cliente_airflow]              -- 226
UNION ALL SELECT 'dim_oferta',  COUNT(*) FROM [gold].[Voomp_Dim_Oferta_airflow]               -- 56
UNION ALL SELECT 'dim_produto', COUNT(*) FROM [gold].[Voomp_Dim_Produto_airflow];             -- 9
GO
