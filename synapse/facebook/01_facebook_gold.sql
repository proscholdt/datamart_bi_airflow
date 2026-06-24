/* ============================================================================
   Synapse Serverless — tabelas externas Delta da GOLD do facebook (pipeline novo).

   Padrão: DB [biitvalleyschool], schema [gold], data source abfss
   [AzureDataLakeStore_DataMartBI_Gold] + file format [DeltaFormat] (os mesmos do
   voomp — IF NOT EXISTS abaixo só p/ este script rodar sozinho).

   6 tabelas em datamartbigold/facebook/ (todas Delta reader v1 → Synapse lê):
     f_camp / dim_camp / f_adset / dim_adset / f_ad / dim_ad
   Fatos (f_*): IDs (texto) + 6 medidas FLOAT + 5 contadores BIGINT + [data] DATE.

   PRÉ-REQUISITO: o Service Principal [Service_Principal_2025_2027] precisa de
   leitura no container datamartbigold.
   ============================================================================ */
USE biitvalleyschool;
GO

-- Data source + file format (idempotentes, compartilhados com o voomp).
IF NOT EXISTS (SELECT 1 FROM sys.external_data_sources WHERE name = 'AzureDataLakeStore_DataMartBI_Gold')
    CREATE EXTERNAL DATA SOURCE [AzureDataLakeStore_DataMartBI_Gold]
    WITH (
        LOCATION   = 'abfss://datamartbigold@saactivecampaign.dfs.core.windows.net/',
        CREDENTIAL = [Service_Principal_2025_2027]
    );
GO
IF NOT EXISTS (SELECT 1 FROM sys.external_file_formats WHERE name = 'DeltaFormat')
    CREATE EXTERNAL FILE FORMAT [DeltaFormat] WITH (FORMAT_TYPE = DELTA);
GO

/* ----------------------------- CAMPANHA ---------------------------------- */
IF OBJECT_ID('gold.Facebook_F_Camp_airflow') IS NOT NULL
    DROP EXTERNAL TABLE [gold].[Facebook_F_Camp_airflow];
GO
CREATE EXTERNAL TABLE [gold].[Facebook_F_Camp_airflow]
(
    [campaign_id]            VARCHAR(50),
    [spend]                  FLOAT,
    [ctr]                    FLOAT,
    [cpc]                    FLOAT,
    [frequency]              FLOAT,
    [cost_per_unique_click]  FLOAT,
    [purchase_value]         FLOAT,
    [impressions]            BIGINT,
    [clicks]                 BIGINT,
    [reach]                  BIGINT,
    [leads]                  BIGINT,
    [purchases]              BIGINT,
    [data]                   DATE
)
WITH (LOCATION = '/facebook/f_camp/', DATA_SOURCE = [AzureDataLakeStore_DataMartBI_Gold], FILE_FORMAT = [DeltaFormat]);
GO

IF OBJECT_ID('gold.Facebook_Dim_Camp_airflow') IS NOT NULL
    DROP EXTERNAL TABLE [gold].[Facebook_Dim_Camp_airflow];
GO
CREATE EXTERNAL TABLE [gold].[Facebook_Dim_Camp_airflow]
(
    [campaign_id]    VARCHAR(50),
    [campaign_name]  VARCHAR(4000)
)
WITH (LOCATION = '/facebook/dim_camp/', DATA_SOURCE = [AzureDataLakeStore_DataMartBI_Gold], FILE_FORMAT = [DeltaFormat]);
GO

/* ------------------------------- ADSET ----------------------------------- */
IF OBJECT_ID('gold.Facebook_F_Adset_airflow') IS NOT NULL
    DROP EXTERNAL TABLE [gold].[Facebook_F_Adset_airflow];
GO
CREATE EXTERNAL TABLE [gold].[Facebook_F_Adset_airflow]
(
    [adset_id]               VARCHAR(50),
    [campaign_id]            VARCHAR(50),
    [spend]                  FLOAT,
    [ctr]                    FLOAT,
    [cpc]                    FLOAT,
    [frequency]              FLOAT,
    [cost_per_unique_click]  FLOAT,
    [purchase_value]         FLOAT,
    [impressions]            BIGINT,
    [clicks]                 BIGINT,
    [reach]                  BIGINT,
    [leads]                  BIGINT,
    [purchases]              BIGINT,
    [data]                   DATE
)
WITH (LOCATION = '/facebook/f_adset/', DATA_SOURCE = [AzureDataLakeStore_DataMartBI_Gold], FILE_FORMAT = [DeltaFormat]);
GO

IF OBJECT_ID('gold.Facebook_Dim_Adset_airflow') IS NOT NULL
    DROP EXTERNAL TABLE [gold].[Facebook_Dim_Adset_airflow];
GO
CREATE EXTERNAL TABLE [gold].[Facebook_Dim_Adset_airflow]
(
    [adset_id]                            VARCHAR(50),
    [adset_name]                          VARCHAR(4000),
    [publicos_personalizados_incluidos]   VARCHAR(4000)
)
WITH (LOCATION = '/facebook/dim_adset/', DATA_SOURCE = [AzureDataLakeStore_DataMartBI_Gold], FILE_FORMAT = [DeltaFormat]);
GO

/* -------------------------------- AD ------------------------------------- */
IF OBJECT_ID('gold.Facebook_F_Ad_airflow') IS NOT NULL
    DROP EXTERNAL TABLE [gold].[Facebook_F_Ad_airflow];
GO
CREATE EXTERNAL TABLE [gold].[Facebook_F_Ad_airflow]
(
    [ad_id]                  VARCHAR(50),
    [adset_id]               VARCHAR(50),
    [campaign_id]            VARCHAR(50),
    [spend]                  FLOAT,
    [ctr]                    FLOAT,
    [cpc]                    FLOAT,
    [frequency]              FLOAT,
    [cost_per_unique_click]  FLOAT,
    [purchase_value]         FLOAT,
    [impressions]            BIGINT,
    [clicks]                 BIGINT,
    [reach]                  BIGINT,
    [leads]                  BIGINT,
    [purchases]              BIGINT,
    [data]                   DATE
)
WITH (LOCATION = '/facebook/f_ad/', DATA_SOURCE = [AzureDataLakeStore_DataMartBI_Gold], FILE_FORMAT = [DeltaFormat]);
GO

IF OBJECT_ID('gold.Facebook_Dim_Ad_airflow') IS NOT NULL
    DROP EXTERNAL TABLE [gold].[Facebook_Dim_Ad_airflow];
GO
CREATE EXTERNAL TABLE [gold].[Facebook_Dim_Ad_airflow]
(
    [ad_id]                VARCHAR(50),
    [ad_name]              VARCHAR(4000),
    [video_url]            VARCHAR(4000),
    [video_url_click]      VARCHAR(4000),
    [video_thumbnail_url]  VARCHAR(4000)
)
WITH (LOCATION = '/facebook/dim_ad/', DATA_SOURCE = [AzureDataLakeStore_DataMartBI_Gold], FILE_FORMAT = [DeltaFormat]);
GO

/* ------------------------------- TESTES ---------------------------------- */
SELECT 'f_camp'    AS tabela, COUNT(*) AS linhas FROM [gold].[Facebook_F_Camp_airflow]      -- 424
UNION ALL SELECT 'dim_camp',  COUNT(*) FROM [gold].[Facebook_Dim_Camp_airflow]              -- 37
UNION ALL SELECT 'f_adset',   COUNT(*) FROM [gold].[Facebook_F_Adset_airflow]               -- 1085
UNION ALL SELECT 'dim_adset', COUNT(*) FROM [gold].[Facebook_Dim_Adset_airflow]             -- 119
UNION ALL SELECT 'f_ad',      COUNT(*) FROM [gold].[Facebook_F_Ad_airflow]                  -- 200
UNION ALL SELECT 'dim_ad',    COUNT(*) FROM [gold].[Facebook_Dim_Ad_airflow];               -- 43
GO
