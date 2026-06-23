"""Metadados do data mart voomp (Delta overwrite snapshot).

Fluxo: Excel dropado na INBOX (stage) → ingest valida + copia p/ RAW (histórico
por ingestion_date) → bronze (Delta overwrite) → silver (MD5 + regras, overwrite)
→ gold (f_vendas + 4 dims, overwrite). Projeções: ramo opcional análogo.

Nomes de coluna preservam o original do Excel (espaços/acentos/barras) — delta-rs
aceita; NÃO usar column mapping (quebra o reader do polars).
"""
from voomp_config import BRONZE, SILVER, GOLD
from common.delta_io import delta_uri

# INBOX (stage) onde o Excel é dropado + prefixo correspondente na RAW (histórico).
INBOX_VENDAS = "source_voomp/voomp"
INBOX_PROJETADAS = "source_voomp/projetadas_voomp"
RAW_VENDAS = "source_voomp/voomp"
RAW_PROJETADAS = "source_voomp/projetadas_voomp"

SHEET_VENDAS = "Exportação de vendas"
VENDAS_COLS_OBRIGATORIAS = ["Nome do comprador", "Email do comprador", "ID Venda"]
IDS_REMOVIDOS = [445793, 445784]  # ID Venda removidos na silver (regra existente)


# --- URIs das tabelas Delta ---
def bronze_vendas_uri():
    return delta_uri(BRONZE, "voomp/bronze_vendas")


def silver_vendas_uri():
    return delta_uri(SILVER, "voomp/silver_vendas")


def f_vendas_uri():
    return delta_uri(GOLD, "voomp/f_vendas")


def dim_uri(name):
    return delta_uri(GOLD, f"voomp/{name}")


def bronze_projetadas_uri():
    return delta_uri(BRONZE, "voomp/bronze_projetadas")


def projetadas_uri():
    return delta_uri(GOLD, "voomp/projetadas")


# --- Colunas do fato f_vendas (grão = ID Venda) ---
F_VENDAS_COLS = [
    "ID Venda", "Data da venda", "Data de pagamento", "Data de vencimento do boleto",
    "Data liberação do saldo", "ID Produto", "ID Oferta", "Método de pagamento",
    "Forma de pagamento", "Cupom", "Valor Oferta", "Valor Pago", "Taxa Voomp",
    "Valor comissão afiliado", "Valor comissão co-produtor", "Valor Recebido",
    "Status da venda", "Motivo do reembolso", "Motivo da recusa", "Venda inteligente",
    "Tipo de cobrança", "ID Contrato", "Status de Contrato", "Período",
    "Recorrência atual", "Recorrência total", "Assinaturas em atraso (dias)",
    "Nota fiscal", "Order Bump", "UF Origem", "Taxa de câmbio", "Link Boleto",
    "Cod. de Barras", "ID_Afiliado", "ID_Cliente",
]

# --- Dimensões: nome → colunas selecionadas (dedup por unique() das colunas) ---
DIM_DEFS = {
    "dim_afiliado": ["ID_Afiliado", "Nome Afiliado"],
    "dim_cliente": [
        "Nome do comprador", "Email do comprador", "Número de telefone",
        "Endereço físico", "ID_Cliente", "CPF/CNPJ",
    ],
    "dim_oferta": ["ID Oferta", "Nome da oferta"],
    "dim_produto": ["ID Produto", "Nome do produto", "Categoria", "Tipo do produto"],
}
