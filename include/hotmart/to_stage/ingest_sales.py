"""Ingestão Hotmart — VENDAS (sales/history → STAGE).

Janela incremental (watermark do bronze − lookback, ou backfill na 1ª carga),
fatiada de 90 em 90 dias (limite da API) e paginada. Achata cada venda e grava
1 JSON por dia (order_date) no stage.
"""
import collections
from datetime import datetime

from hotmart_config import get_container_client
from hotmart_meta import SALES, get_access_token, paged, ms, sales_chunks, flatten_sale
from to_stage.stage_io import write_stage

get_container_client("stage")  # zona de pouso


def _day(ms_val):
    return datetime.fromtimestamp(ms_val / 1000).date().isoformat() if ms_val else None


def ingest_sales():
    token = get_access_token()
    chunks = sales_chunks()
    print(f"📅 janela sales: {chunks[0][0]} → {chunks[-1][1]} ({len(chunks)} fatias de 90d)")
    rows = []
    for a, b in chunks:
        params = {"start_date": ms(a), "end_date": ms(b), "max_results": 100}
        items = paged("sales/history", token, params, f"sales[{a}]")
        rows += [flatten_sale(r) for r in items]
        print(f"  {a}..{b}: +{len(items)} (total {len(rows)})")

    by_tx = {r["transaction"]: r for r in rows if r.get("transaction")}  # dedup defensivo
    by_day = collections.defaultdict(list)
    for r in by_tx.values():
        d = _day(r.get("order_date_ms"))
        if d:
            by_day[d].append(r)

    for d in sorted(by_day):
        write_stage(SALES, by_day[d], d)
    print(f"✅ ingest_sales: {len(by_tx)} vendas em {len(by_day)} dia(s) → stage")


if __name__ == "__main__":
    ingest_sales()
