"""bronze → silver (facebook): lê SÓ a janela do bronze, dedup/conforma, MERGE por grão."""
from common.delta_io import merge_delta, scan_window, window_start
from fb_meta import ENTITIES, LOOKBACK_DAYS, bronze_uri, silver_uri, grain_predicate


def bronze_to_silver(entity):
    cfg = ENTITIES[entity]
    start = window_start(silver_uri(entity), LOOKBACK_DAYS)
    df = scan_window(bronze_uri(entity), "data", start).collect()
    if df.is_empty():
        print(f"janela vazia no bronze_{entity} (>= {start})")
        return
    # silver conformada: dedup por grão (limpeza/regra entram aqui se houver)
    df = df.unique(subset=cfg["grain"], keep="last")
    merge_delta(df, silver_uri(entity), grain_predicate(entity), partition_by=["data"])
    print(f"✅ silver_{entity}: {df.height} linhas (>= {start})")
