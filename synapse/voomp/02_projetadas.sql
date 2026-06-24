/* ============================================================================
   Synapse Serverless — tabela externa Delta da gold do voomp: PROJETADAS.

   Mesmo padrão do 01_setup_e_f_vendas.sql: DB [biitvalleyschool], schema [gold],
   data source abfss [AzureDataLakeStore_DataMartBI_Gold] + [DeltaFormat].
   Reaproveita o data source/file format já criados no script 01 (os IF NOT EXISTS
   abaixo são só p/ este script rodar sozinho também).

   Tabela: datamartbigold/voomp/projetadas  (Delta, reader v1 → Synapse lê).
   Colunas (schema real):
     - [Data]  : TEXTO no formato DD/MM/YYYY (não é date nativo).
     - [Valor] : Decimal(18,2).
   ============================================================================ */
USE biitvalleyschool;
GO

-- 1) Tabela externa: dropar PRIMEIRO (depende do data source).
IF OBJECT_ID('gold.Voomp_Projetadas_airflow') IS NOT NULL
    DROP EXTERNAL TABLE [gold].[Voomp_Projetadas_airflow];
GO

-- 2) Data source — idempotente (compartilhado com as outras tabelas da gold).
IF NOT EXISTS (SELECT 1 FROM sys.external_data_sources WHERE name = 'AzureDataLakeStore_DataMartBI_Gold')
    CREATE EXTERNAL DATA SOURCE [AzureDataLakeStore_DataMartBI_Gold]
    WITH (
        LOCATION   = 'abfss://datamartbigold@saactivecampaign.dfs.core.windows.net/',
        CREDENTIAL = [Service_Principal_2025_2027]
    );
GO

-- 3) File format DELTA — idempotente.
IF NOT EXISTS (SELECT 1 FROM sys.external_file_formats WHERE name = 'DeltaFormat')
    CREATE EXTERNAL FILE FORMAT [DeltaFormat] WITH (FORMAT_TYPE = DELTA);
GO

-- 4) (Re)cria a tabela externa.
CREATE EXTERNAL TABLE [gold].[Voomp_Projetadas_airflow]
(
    [Data]   VARCHAR(10),       -- texto DD/MM/YYYY
    [Valor]  DECIMAL(18,2)
)
WITH (
    LOCATION    = '/voomp/projetadas/',
    DATA_SOURCE = [AzureDataLakeStore_DataMartBI_Gold],
    FILE_FORMAT = [DeltaFormat]
);
GO

-- 5) Teste.
SELECT COUNT(*) AS linhas FROM [gold].[Voomp_Projetadas_airflow];   -- esperado: 13
SELECT * FROM [gold].[Voomp_Projetadas_airflow];

-- Opcional: ordenar cronologicamente convertendo o texto DD/MM/YYYY -> date (estilo 103).
SELECT [Data], [Valor]
FROM [gold].[Voomp_Projetadas_airflow]
ORDER BY CONVERT(date, [Data], 103);
GO
