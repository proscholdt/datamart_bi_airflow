"""Ingestão Hotmart — ASSINATURAS (subscriptions → STAGE), snapshot do estado atual.

A conta não tem assinaturas hoje (0) → flatten GENÉRICO (achata o que vier, sem
depender do schema exato). Refinar a modelagem quando houver dado real.
"""
from hotmart_config import get_container_client
from hotmart_meta import SUBS, get_access_token, paged, flatten_generic
from to_stage.stage_io import write_stage

get_container_client("stage")


def ingest_subscriptions():
    token = get_access_token()
    items = paged("subscriptions", token, {"max_results": 100}, "subscriptions")
    rows = [flatten_generic(r) for r in items]
    write_stage(SUBS, rows, "snapshot")  # snapshot único (overwrite a cada run)
    print(f"✅ ingest_subscriptions: {len(rows)} assinaturas → stage (snapshot)")


if __name__ == "__main__":
    ingest_subscriptions()
