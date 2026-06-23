

import requests
import os
import json
from datetime import datetime, timedelta
from azure.storage.blob import ContentSettings

from facebook_config import get_blob_service_client, get_container_client
from fb_meta import api_window
from to_raw.raw_io import write_raw, ingestion_date

# Facebook
ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")
AD_ACCOUNT_ID = os.getenv("FB_AD_ACCOUNT_ID")

ENTITY = "adset"

# Zona RAW (datamartbiraw, auto-criada)
get_container_client("raw")  # destino da ingestão é a RAW (JSON append-only)
blob_service_client = get_blob_service_client()
ING_DATE = ingestion_date()

# ------------- NOVO: helpers para coletar públicos por Ad Set -------------
def _api_get_paged(url: str, params: dict):
    out = []
    while True:
        r = requests.get(url, params=params)
        if r.status_code != 200:
            raise Exception(f"Erro na API do Facebook: {r.status_code} - {r.text}")
        payload = r.json()
        out.extend(payload.get("data", []))
        after = payload.get("paging", {}).get("cursors", {}).get("after")
        if not after:
            break
        params = dict(params)
        params["after"] = after
    return out

def _extract_custom_aud_names_from_targeting(targeting: dict):
    """
    Retorna lista com nomes/ids de públicos personalizados incluídos.
    Varre custom_audiences diretas e dentro de flexible_spec.
    """
    if not targeting:
        return []

    names, ids = set(), set()

    def _push(lst):
        for ca in (lst or []):
            name = ca.get("name")
            cid = ca.get("id")
            if name:
                names.add(name)
            elif cid:
                ids.add(cid)

    # Direto em targeting
    _push(targeting.get("custom_audiences"))

    # Em flexible_spec (comum para lookalike/custom audiences)
    for spec in (targeting.get("flexible_spec") or []):
        _push(spec.get("custom_audiences"))

    result = []
    if names:
        result.extend(sorted(names))
    if ids:  # fallback quando não vier o nome
        result.extend([f"CA:{cid}" for cid in sorted(ids)])
    return result

def build_adset_custom_audience_map():
    """
    Mapeia adset_id -> string com públicos personalizados incluídos.
    """
    url = f"https://graph.facebook.com/v19.0/{AD_ACCOUNT_ID}/adsets"
    params = {
        "access_token": ACCESS_TOKEN,
        "fields": "id,name,targeting,effective_status",
        "limit": 200
    }
    adsets = _api_get_paged(url, params)

    mapping = {}
    for adset in adsets:
        audiences = _extract_custom_aud_names_from_targeting(adset.get("targeting"))
        mapping[str(adset.get("id"))] = " | ".join(audiences) if audiences else ""
    return mapping
# --------------------------------------------------------------------------

# Função principal
def fetch_facebook_adsets_by_day(start_date, end_date, only_with_data=False):
    print("Gerando arquivos por dia...")

    # NOVO: carrega o mapa de públicos personalizados por Ad Set uma única vez
    print("Carregando públicos personalizados por Ad Set...")
    adset_custom_map = build_adset_custom_audience_map()

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    delta = timedelta(days=1)

    while start <= end:
        date_str = start.strftime("%Y-%m-%d")
        print(f"Processando {date_str}...")

        data = fetch_facebook_adsets_direct(
            start_date=date_str,
            end_date=date_str,
            only_with_data=only_with_data,
            export_filename=f"adsets_{date_str}.json",
            adset_custom_map=adset_custom_map  # NOVO
        )

        print(f"{date_str}: {len(data)} registros")
        start += delta

# Função que faz a requisição e envia ao Blob
def fetch_facebook_adsets_direct(start_date, end_date, only_with_data=False, export_filename=None, adset_custom_map=None):
    required_fields = [
        "adset_id", "adset_name", "campaign_id", "campaign_name",
        "date_start", "date_stop", "spend", "impressions", "clicks",
        "ctr", "cpc", "reach", "frequency", "cost_per_unique_click",
        "actions"
    ]

    url = f"https://graph.facebook.com/v19.0/{AD_ACCOUNT_ID}/insights"
    params = {
        "access_token": ACCESS_TOKEN,
        "time_range": json.dumps({"since": start_date, "until": end_date}),
        "level": "adset",
        "fields": ",".join(required_fields),
        "limit": 500
    }

    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise Exception(f"Erro na API do Facebook: {response.status_code} - {response.text}")

    data = response.json().get("data", [])

    # Extrai ações e calcula valores; injeta a nova coluna
    for item in data:
        actions = item.pop("actions", [])
        item["leads"] = get_action_value(actions, "lead")
        item["purchases"] = get_action_value(actions, "purchase")
        item["purchase_value"] = get_action_value(actions, "omni_purchase", as_float=True)

        # NOVO: adiciona 'publicos_personalizados_incluidos' por adset_id
        adset_id = str(item.get("adset_id", "")) if item.get("adset_id") is not None else ""
        if adset_custom_map:
            item["publicos_personalizados_incluidos"] = adset_custom_map.get(adset_id, "")
        else:
            item["publicos_personalizados_incluidos"] = ""

    campos_padrao = {
        "date_start": start_date,
        "date_stop": end_date,
        "spend": 0.0,
        "impressions": 0,
        "clicks": 0,
        "ctr": 0.0,
        "cpc": 0.0,
        "reach": 0,
        "frequency": 0.0,
        "cost_per_unique_click": 0.0,
        "leads": 0,
        "purchases": 0,
        "purchase_value": 0.0,
        "publicos_personalizados_incluidos": ""  
    }

    for item in data:
        for campo, valor_padrao in campos_padrao.items():
            if campo not in item:
                item[campo] = valor_padrao

    if only_with_data:
        data = [d for d in data if any([
            float(d.get("spend", 0)) > 0,
            int(d.get("impressions", 0)) > 0,
            int(d.get("clicks", 0)) > 0,
            int(d.get("leads", 0)) > 0,
            int(d.get("purchases", 0)) > 0,
            float(d.get("purchase_value", 0)) > 0
        ])]

    if not export_filename:
        export_filename = f"adsets_{start_date}.json"

    # Pula dias vazios: não grava arquivo "[]" na raw (a validação exige não-vazio).
    if not data:
        print(f"⏭️  {start_date}: sem dados — nada gravado na raw")
        return data

    # Validação (staging in-task): todo registro não-vazio deve ter o id da entidade.
    if any("adset_id" not in d for d in data):
        raise Exception(f"❌ Validação: registro sem adset_id em {start_date}")

    write_raw(ENTITY, data, start_date, ING_DATE)
    return data

def get_action_value(actions, action_type, as_float=False):
    for action in actions:
        if action.get("action_type") == action_type:
            try:
                return float(action["value"]) if as_float else int(float(action["value"]))
            except:
                return 0.0 if as_float else 0
    return 0.0 if as_float else 0

# Execução
if __name__ == "__main__":
    # Janela = watermark(bronze) - lookback até D-1 (ou FB_LOAD_START/END p/ backfill).
    start_date, end_date = api_window(ENTITY)
    print(f"📅 Janela de ingestão (adset): {start_date} → {end_date}  [ingestion_date={ING_DATE}]")
    fetch_facebook_adsets_by_day(
        start_date=start_date,
        end_date=end_date,
        only_with_data=True,
    )
