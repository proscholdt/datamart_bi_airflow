"""silver → gold (facebook): FATO (delete+append por janela) + DIM (MERGE por id, SCD-1).

O fato NÃO é particionado por `data`: o delete da janela já é por predicado
(`data >= start`), e manter `data` como coluna física (não de partição) é o que
o Synapse serverless consegue ler — coluna de partição Delta vem NULL no Synapse.
"""
from common.delta_io import scan_window, window_start, delete_and_append, merge_delta
from fb_meta import (
    ENTITIES,
    LOOKBACK_DAYS,
    MEASURE_FLOAT,
    MEASURE_INT,
    silver_uri,
    fact_uri,
    dim_uri,
    dim_predicate,
)


def silver_to_gold(entity):
    cfg = ENTITIES[entity]

    # --- FATO (grão atual data+id, sem rollup): delete janela + append ---
    fstart = window_start(fact_uri(entity), LOOKBACK_DAYS)
    sdf = scan_window(silver_uri(entity), "data", fstart).collect()
    if sdf.is_empty():
        print(f"janela vazia no silver_{entity} (>= {fstart})")
        return

    fact_cols = cfg["grain"] + cfg["fk_cols"] + (MEASURE_FLOAT + MEASURE_INT)
    fact = (
        sdf.select([c for c in fact_cols if c in sdf.columns])
        .unique(subset=cfg["grain"], keep="last")
    )
    delete_and_append(fact, fact_uri(entity), f"data >= '{fstart}'")
    print(f"✅ f_{entity}: {fact.height} linhas (janela >= {fstart})")

    # --- DIMENSÃO (SCD-1 por id): MERGE (atualiza atributos, insere novos) ---
    dcols = [c for c in cfg["dim_cols"] if c in sdf.columns]
    ddf = sdf.select(dcols).unique(subset=cfg["dim_key"], keep="last")
    merge_delta(ddf, dim_uri(entity), dim_predicate(entity))
    print(f"✅ {cfg['dim_name']}: {ddf.height} linhas (MERGE por id)")
