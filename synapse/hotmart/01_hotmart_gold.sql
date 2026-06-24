/* ============================================================================
   Synapse Serverless — tabelas externas Delta da GOLD do hotmart.

   Padrão: DB [biitvalleyschool], schema [gold], data source abfss
   [AzureDataLakeStore_DataMartBI_Gold] + file format [DeltaFormat] (mesmos do
   voomp/facebook — IF NOT EXISTS só p/ rodar sozinho). Tudo reader v1, datas DATE,
   fatos NÃO particionados (coluna de partição Delta vem NULL no Synapse).

   Inclui: f_vendas + dim_cliente/produto/oferta/produtor.
   f_assinaturas: a conta NÃO tem assinaturas hoje → a tabela Delta ainda não existe;
   gere a externa quando houver dado (schema a confirmar com os campos reais).
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

/* ------------------------------ F_VENDAS --------------------------------- */
IF OBJECT_ID('gold.Hotmart_F_Vendas_airflow') IS NOT NULL DROP EXTERNAL TABLE [gold].[Hotmart_F_Vendas_airflow];
GO
CREATE EXTERNAL TABLE [gold].[Hotmart_F_Vendas_airflow]
(
    [transaction]            VARCHAR(50),
    [data]                   DATE,          -- order_date
    [approved_date]          DATE,
    [warranty_expire_date]   DATE,
    [status]                 VARCHAR(50),
    [is_subscription]        BIT,
    [commission_as]          VARCHAR(50),
    [product_id]             BIGINT,
    [buyer_ucode]            VARCHAR(100),
    [producer_ucode]         VARCHAR(100),
    [offer_code]             VARCHAR(100),
    [payment_type]           VARCHAR(50),
    [payment_method]         VARCHAR(50),
    [installments_number]    INT,
    [price_currency]         VARCHAR(10),
    [price_value]            FLOAT,
    [fee_base]               FLOAT,
    [fee_fixed]              FLOAT,
    [fee_total]              FLOAT,
    [fee_percentage]         INT,
    [valor_liquido]          FLOAT
)
WITH (LOCATION = '/hotmart/f_vendas/', DATA_SOURCE = [AzureDataLakeStore_DataMartBI_Gold], FILE_FORMAT = [DeltaFormat]);
GO

/* ------------------------------ DIMENSÕES -------------------------------- */
IF OBJECT_ID('gold.Hotmart_Dim_Cliente_airflow') IS NOT NULL DROP EXTERNAL TABLE [gold].[Hotmart_Dim_Cliente_airflow];
GO
CREATE EXTERNAL TABLE [gold].[Hotmart_Dim_Cliente_airflow]
( [buyer_ucode] VARCHAR(100), [buyer_name] VARCHAR(4000), [buyer_email] VARCHAR(4000) )
WITH (LOCATION = '/hotmart/dim_cliente/', DATA_SOURCE = [AzureDataLakeStore_DataMartBI_Gold], FILE_FORMAT = [DeltaFormat]);
GO

IF OBJECT_ID('gold.Hotmart_Dim_Produto_airflow') IS NOT NULL DROP EXTERNAL TABLE [gold].[Hotmart_Dim_Produto_airflow];
GO
CREATE EXTERNAL TABLE [gold].[Hotmart_Dim_Produto_airflow]
( [product_id] BIGINT, [product_name] VARCHAR(4000) )
WITH (LOCATION = '/hotmart/dim_produto/', DATA_SOURCE = [AzureDataLakeStore_DataMartBI_Gold], FILE_FORMAT = [DeltaFormat]);
GO

IF OBJECT_ID('gold.Hotmart_Dim_Oferta_airflow') IS NOT NULL DROP EXTERNAL TABLE [gold].[Hotmart_Dim_Oferta_airflow];
GO
CREATE EXTERNAL TABLE [gold].[Hotmart_Dim_Oferta_airflow]
( [offer_code] VARCHAR(100), [offer_payment_mode] VARCHAR(50) )
WITH (LOCATION = '/hotmart/dim_oferta/', DATA_SOURCE = [AzureDataLakeStore_DataMartBI_Gold], FILE_FORMAT = [DeltaFormat]);
GO

IF OBJECT_ID('gold.Hotmart_Dim_Produtor_airflow') IS NOT NULL DROP EXTERNAL TABLE [gold].[Hotmart_Dim_Produtor_airflow];
GO
CREATE EXTERNAL TABLE [gold].[Hotmart_Dim_Produtor_airflow]
( [producer_ucode] VARCHAR(100), [producer_name] VARCHAR(4000) )
WITH (LOCATION = '/hotmart/dim_produtor/', DATA_SOURCE = [AzureDataLakeStore_DataMartBI_Gold], FILE_FORMAT = [DeltaFormat]);
GO

/* ------------------------------- TESTES ---------------------------------- */
SELECT 'f_vendas' AS tabela, COUNT(*) AS linhas FROM [gold].[Hotmart_F_Vendas_airflow]
UNION ALL SELECT 'dim_cliente',  COUNT(*) FROM [gold].[Hotmart_Dim_Cliente_airflow]
UNION ALL SELECT 'dim_produto',  COUNT(*) FROM [gold].[Hotmart_Dim_Produto_airflow]
UNION ALL SELECT 'dim_oferta',   COUNT(*) FROM [gold].[Hotmart_Dim_Oferta_airflow]
UNION ALL SELECT 'dim_produtor', COUNT(*) FROM [gold].[Hotmart_Dim_Produtor_airflow];
GO
