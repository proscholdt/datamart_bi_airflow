/* ============================================================================
   Synapse Serverless — tabela externa Delta da gold do voomp (pipeline novo).

   Segue o padrão já usado no ambiente: DB [biitvalleyschool], schema [gold],
   credencial Service Principal [Service_Principal_2025_2027], data source abfss
   por container. NOVIDADES vs. as tabelas antigas:
     - os dados novos são DELTA (não parquet solto) → FILE_FORMAT = DELTA.
     - vivem no container datamartbigold (não no antigo "gold").

   PRÉ-REQUISITOS:
     1. A credencial [Service_Principal_2025_2027] já existe (criada à parte com
        o secret — NÃO repetir o secret aqui).
     2. O Service Principal precisa de LEITURA no container datamartbigold
        (Storage Blob Data Reader na conta saactivecampaign OU no container).
        Se ele só tinha acesso aos containers antigos (gold/silver/bronze),
        conceda também no datamartbigold.
   ============================================================================ */
USE biitvalleyschool;
GO

-- 1) Data source para o container datamartbigold (onde está o Delta novo).
IF EXISTS (SELECT 1 FROM sys.external_data_sources WHERE name = 'AzureDataLakeStore_DataMartBI_Gold')
    DROP EXTERNAL DATA SOURCE [AzureDataLakeStore_DataMartBI_Gold];
GO
CREATE EXTERNAL DATA SOURCE [AzureDataLakeStore_DataMartBI_Gold]
WITH (
    LOCATION   = 'abfss://datamartbigold@saactivecampaign.dfs.core.windows.net/',
    CREDENTIAL = [Service_Principal_2025_2027]
);
GO

-- 2) File format DELTA (você só tinha ParquetFormat). Reutilizável p/ as 6 tabelas.
IF NOT EXISTS (SELECT 1 FROM sys.external_file_formats WHERE name = 'DeltaFormat')
    CREATE EXTERNAL FILE FORMAT [DeltaFormat] WITH (FORMAT_TYPE = DELTA);
GO

-- 3) Tabela externa. Mantém [gold].[Voomp_F_Vendas], mas REAPONTA para o Delta
--    novo (datamartbigold/voomp/f_vendas). Tipos espelham o schema real do Delta.
IF OBJECT_ID('gold.Voomp_F_Vendas') IS NOT NULL
    DROP EXTERNAL TABLE [gold].[Voomp_F_Vendas];
GO
CREATE EXTERNAL TABLE [gold].[Voomp_F_Vendas]
(
    [ID Venda]                       BIGINT,        -- Int64  (era INT)
    [Data da venda]                  DATETIME2(6),  -- timestamp us (era DATETIME)
    [Data de pagamento]              DATETIME2(6),
    [Data de vencimento do boleto]   DATE,
    [Data liberação do saldo]        DATETIME2(6),
    [ID Produto]                     BIGINT,        -- Int64
    [ID Oferta]                      VARCHAR(4000),
    [Método de pagamento]            VARCHAR(4000),
    [Forma de pagamento]             BIGINT,        -- Int64
    [Cupom]                          VARCHAR(4000),
    [Valor Oferta]                   FLOAT,         -- Float64 (era INT!)
    [Valor Pago]                     FLOAT,         -- Float64 (era INT!)
    [Taxa Voomp]                     FLOAT,
    [Valor comissão afiliado]        FLOAT,
    [Valor comissão co-produtor]     VARCHAR(4000),
    [Valor Recebido]                 FLOAT,
    [Status da venda]                VARCHAR(4000),
    [Motivo do reembolso]            VARCHAR(4000),
    [Motivo da recusa]               VARCHAR(4000),
    [Venda inteligente]              VARCHAR(4000),
    [Tipo de cobrança]               VARCHAR(4000),
    [ID Contrato]                    BIGINT,        -- Int64
    [Status de Contrato]             VARCHAR(4000),
    [Período]                        VARCHAR(4000),
    [Recorrência atual]              BIGINT,        -- Int64
    [Recorrência total]              BIGINT,        -- Int64
    [Assinaturas em atraso (dias)]   VARCHAR(4000),
    [Nota fiscal]                    BIGINT,        -- Int64
    [Order Bump]                     VARCHAR(4000),
    [UF Origem]                      VARCHAR(4000),
    [Taxa de câmbio]                 FLOAT,         -- Float64 (era VARCHAR!)
    [Link Boleto]                    VARCHAR(4000),
    [Cod. de Barras]                 VARCHAR(4000),
    [ID_Afiliado]                    VARCHAR(100),  -- MD5 hex (32)
    [ID_Cliente]                     VARCHAR(100)
    -- OBS: [Taxa de parcelamento] da tabela antiga NÃO existe no Delta novo
    --      (não entrou na lista de colunas do f_vendas migrado). Se precisar,
    --      eu adiciono em voomp_meta.F_VENDAS_COLS e re-rodo o DAG.
)
WITH (
    LOCATION    = '/voomp/f_vendas/',
    DATA_SOURCE = [AzureDataLakeStore_DataMartBI_Gold],
    FILE_FORMAT = [DeltaFormat]
);
GO

-- 4) Teste.
SELECT COUNT(*) AS linhas FROM [gold].[Voomp_F_Vendas];   -- esperado: 1174
SELECT TOP 50 * FROM [gold].[Voomp_F_Vendas];
GO
