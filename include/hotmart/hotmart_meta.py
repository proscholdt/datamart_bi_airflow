"""Metadados do data mart Hotmart (Delta, incremental por MERGE).

Fonte: API Hotmart Payments v1.
  - sales/history  → vendas (grão = transaction), incremental: watermark(bronze.data)
    − lookback até hoje, fatiado de 90 em 90 dias (limite da API), MERGE por transaction.
  - subscriptions  → assinaturas (snapshot do estado atual), overwrite. SEM dado real
    na conta hoje (0 assinaturas) → flatten genérico, modelagem a refinar quando houver.

Auth: OAuth client_credentials. As credenciais vêm de HOTMART_CLIENT_ID /
HOTMART_CLIENT_SECRET (renomeie as chaves 'Client ID'/'Client Secret' do .env, que
têm espaço e o astro não carrega). Fallback: deriva do HOTMART_BASIC_TOKEN
(base64 de client_id:client_secret; o valor no .env já vem com o prefixo 'Basic ').
"""
import base64
import os
import time
from datetime import date, datetime, timedelta

import requests

from hotmart_config import BRONZE, SILVER, GOLD
from common.delta_io import delta_uri, read_watermark

# --------------------------------------------------------------------------- #
# Fontes / zonas
# --------------------------------------------------------------------------- #
SALES = "sales"
SUBS = "subscriptions"
STAGE_DIR = {SALES: "source_hotmart/sales", SUBS: "source_hotmart/subscriptions"}
STEM = {SALES: "sales", SUBS: "subscriptions"}

# Janela de restatement da Hotmart (status muda: APPROVED→COMPLETE, reembolso/chargeback).
LOOKBACK_DAYS = int(os.getenv("HOTMART_LOOKBACK_DAYS", "90"))
# 1ª carga (bronze vazio): quantos dias retroagir.
DEFAULT_BACKFILL_DAYS = int(os.getenv("HOTMART_BACKFILL_DAYS", "1825"))
# A API rejeita ranges muito largos → fatiar a janela.
CHUNK_DAYS = 90

TOKEN_URL = "https://api-sec-vlc.hotmart.com/security/oauth/token"
API = "https://developers.hotmart.com/payments/api/v1"


# --------------------------------------------------------------------------- #
# URIs Delta
# --------------------------------------------------------------------------- #
def bronze_uri(src):
    return delta_uri(BRONZE, f"hotmart/bronze_{src}")


def silver_uri(src):
    return delta_uri(SILVER, f"hotmart/silver_{src}")


def f_vendas_uri():
    return delta_uri(GOLD, "hotmart/f_vendas")


def f_assinaturas_uri():
    return delta_uri(GOLD, "hotmart/f_assinaturas")


def dim_uri(name):
    return delta_uri(GOLD, f"hotmart/{name}")


# --------------------------------------------------------------------------- #
# Auth + HTTP
# --------------------------------------------------------------------------- #
def _credentials():
    cid, csec = os.getenv("HOTMART_CLIENT_ID"), os.getenv("HOTMART_CLIENT_SECRET")
    if cid and csec:
        return cid, csec
    basic = os.getenv("HOTMART_BASIC_TOKEN", "")
    if basic:
        try:
            dec = base64.b64decode(basic.split()[-1]).decode()  # tira prefixo 'Basic '
            if ":" in dec:
                return dec.split(":", 1)
        except Exception:
            pass
    raise RuntimeError(
        "Credenciais Hotmart ausentes — setar HOTMART_CLIENT_ID/HOTMART_CLIENT_SECRET "
        "(ou HOTMART_BASIC_TOKEN) no ambiente."
    )


def get_access_token():
    cid, csec = _credentials()
    r = requests.post(TOKEN_URL, params={"grant_type": "client_credentials"}, auth=(cid, csec), timeout=60)
    r.raise_for_status()
    return r.json()["access_token"]


def api_get(url, params, token, tag=""):
    """GET autenticado com retry: rate limit/transitório/timeout → dorme e re-tenta."""
    headers = {"Authorization": f"Bearer {token}"}
    for attempt in range(8):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=120)
        except requests.RequestException as e:
            print(f"  [net {tag}] {type(e).__name__} 15s ({attempt+1})"); time.sleep(15); continue
        if r.status_code == 200:
            return r.json()
        low = r.text.lower()
        if r.status_code in (429, 500, 502, 503, 504) or "rate" in low or "timeout" in low or "unavailable" in low:
            print(f"  [retry {tag}] {r.status_code} 20s ({attempt+1})"); time.sleep(20); continue
        raise Exception(f"Erro API Hotmart ({tag}): {r.status_code} - {r.text[:300]}")
    raise Exception(f"Falha persistente em {tag}")


def paged(path, token, params, tag, cap=100000):
    """Itera todas as páginas (page_info.next_page_token) acumulando items."""
    items = []
    p = dict(params)
    while True:
        j = api_get(f"{API}/{path}", p, token, tag)
        items += j.get("items", [])
        tok = (j.get("page_info") or {}).get("next_page_token")
        if not tok or len(items) >= cap:
            break
        p = dict(params); p["page_token"] = tok
    return items


# --------------------------------------------------------------------------- #
# Janela incremental (sales): watermark(bronze.data) − lookback, fatiada em 90d
# --------------------------------------------------------------------------- #
def sales_chunks():
    """Lista de (start_date, end_date) cobrindo a janela a re-extrair, em fatias de 90d."""
    wm = read_watermark(bronze_uri(SALES), "data")
    start = (wm - timedelta(days=LOOKBACK_DAYS)) if wm else (date.today() - timedelta(days=DEFAULT_BACKFILL_DAYS))
    end = date.today()
    out = []
    a = start
    while a <= end:
        b = min(a + timedelta(days=CHUNK_DAYS - 1), end)
        out.append((a, b)); a = b + timedelta(days=1)
    return out


def ms(d):
    return int(datetime(d.year, d.month, d.day).timestamp() * 1000)


# --------------------------------------------------------------------------- #
# Flatten dos registros da API → dict plano (1 linha por venda)
# --------------------------------------------------------------------------- #
def flatten_sale(rec):
    pu = rec.get("purchase") or {}
    pr = pu.get("price") or {}
    pm = pu.get("payment") or {}
    fe = pu.get("hotmart_fee") or {}
    of = pu.get("offer") or {}
    pd = rec.get("product") or {}
    by = rec.get("buyer") or {}
    pc = rec.get("producer") or {}
    return {
        "transaction": pu.get("transaction"),
        "status": pu.get("status"),
        "order_date_ms": pu.get("order_date"),
        "approved_date_ms": pu.get("approved_date"),
        "warranty_expire_date_ms": pu.get("warranty_expire_date"),
        "is_subscription": pu.get("is_subscription"),
        "commission_as": pu.get("commission_as"),
        "product_id": pd.get("id"),
        "product_name": pd.get("name"),
        "buyer_ucode": by.get("ucode"),
        "buyer_name": by.get("name"),
        "buyer_email": by.get("email"),
        "producer_ucode": pc.get("ucode"),
        "producer_name": pc.get("name"),
        "offer_code": of.get("code"),
        "offer_payment_mode": of.get("payment_mode"),
        "payment_type": pm.get("type"),
        "payment_method": pm.get("method"),
        "installments_number": pm.get("installments_number"),
        "price_currency": pr.get("currency_code"),
        "price_value": pr.get("value"),
        "fee_base": fe.get("base"),
        "fee_fixed": fe.get("fixed"),
        "fee_total": fe.get("total"),
        "fee_percentage": fe.get("percentage"),
        "fee_currency": fe.get("currency_code"),
    }


def flatten_generic(rec, prefix=""):
    """Achata um dict aninhado em colunas `a_b_c` (listas viram JSON). Para subscriptions
    (schema ainda não confirmado — 0 assinaturas na conta)."""
    import json
    out = {}
    for k, v in (rec or {}).items():
        key = f"{prefix}{k}"
        if isinstance(v, dict):
            out.update(flatten_generic(v, key + "_"))
        elif isinstance(v, list):
            out[key] = json.dumps(v, ensure_ascii=False)
        else:
            out[key] = v
    return out


# --------------------------------------------------------------------------- #
# Sales: grão, predicate, colunas do fato e das dimensões
# --------------------------------------------------------------------------- #
SALES_GRAIN = ["transaction"]
SALES_PREDICATE = "t.transaction = s.transaction"

# Datas (ms → DATE no bronze).
SALES_DATE_COLS = {"data": "order_date_ms", "approved_date": "approved_date_ms",
                   "warranty_expire_date": "warranty_expire_date_ms"}
SALES_FLOAT = ["price_value", "fee_base", "fee_fixed", "fee_total"]
SALES_INT = ["product_id", "installments_number", "fee_percentage"]
SALES_STR = ["transaction", "status", "commission_as", "buyer_ucode", "buyer_name",
             "buyer_email", "producer_ucode", "producer_name", "offer_code",
             "offer_payment_mode", "payment_type", "payment_method", "product_name",
             "price_currency", "fee_currency"]

# Fato f_vendas (grão = transaction): FKs + medidas + datas + status (valor_liquido derivado).
F_VENDAS_COLS = [
    "transaction", "data", "approved_date", "warranty_expire_date", "status",
    "is_subscription", "commission_as", "product_id", "buyer_ucode", "producer_ucode",
    "offer_code", "payment_type", "payment_method", "installments_number",
    "price_currency", "price_value", "fee_base", "fee_fixed", "fee_total", "fee_percentage",
]

# Dimensões: nome → (chave, colunas).
DIM_DEFS = {
    "dim_cliente": ("buyer_ucode", ["buyer_ucode", "buyer_name", "buyer_email"]),
    "dim_produto": ("product_id", ["product_id", "product_name"]),
    "dim_oferta": ("offer_code", ["offer_code", "offer_payment_mode"]),
    "dim_produtor": ("producer_ucode", ["producer_ucode", "producer_name"]),
}
