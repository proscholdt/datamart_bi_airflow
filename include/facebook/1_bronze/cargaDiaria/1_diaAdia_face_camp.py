



import requests
import os
import json
from datetime import datetime, timedelta
from azure.storage.blob import ContentSettings

from facebook_config import get_blob_service_client, get_container_client
from fb_meta import api_window, write_raw, ingestion_date

# ================================
# Credenciais e configuração
# ================================
ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")
AD_ACCOUNT_ID = os.getenv("FB_AD_ACCOUNT_ID")

ENTITY = "camp"

# ================================
# Inicializa o Blob Service (zona RAW datamartbiraw, auto-criada)
# ================================
get_container_client("raw")  # destino da ingestão é a RAW (JSON append-only)
blob_service_client = get_blob_service_client()
ING_DATE = ingestion_date()

# ================================
# Funções auxiliares
# ================================
def fetch_facebook_campaigns_by_day(start_date, end_date, only_with_data=False):
    print("📅 Gerando arquivos por dia...")

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    delta = timedelta(days=1)

    while start <= end:
        date_str = start.strftime("%Y-%m-%d")
        print(f"📆 Processando {date_str}...")

        data = fetch_facebook_campaigns_direct(
            start_date=date_str,
            end_date=date_str,
            only_with_data=only_with_data,
            export_filename=f"campaigns_{date_str}.json"
        )

        print(f"✅ {date_str}: {len(data)} registros")
        start += delta

def fetch_facebook_campaigns_direct(start_date, end_date, only_with_data=False, export_filename=None):
    required_fields = [
        "campaign_id", "campaign_name",
        "date_start", "date_stop", "spend", "impressions", "clicks",
        "ctr", "cpc", "reach", "frequency", "cost_per_unique_click",
        "actions"
    ]

    url = f"https://graph.facebook.com/v19.0/{AD_ACCOUNT_ID}/insights"
    params = {
        "access_token": ACCESS_TOKEN,
        "time_range": json.dumps({
            "since": start_date,
            "until": end_date
        }),
        "level": "campaign",
        "fields": ",".join(required_fields),
        "limit": 500
    }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        raise Exception(f"❌ Erro na API do Facebook: {response.status_code} - {response.text}")

    data = response.json().get("data", [])

    for item in data:
        actions = item.pop("actions", [])
        item["leads"] = get_action_value(actions, "lead")
        item["purchases"] = get_action_value(actions, "purchase")
        item["purchase_value"] = get_action_value(actions, "omni_purchase", as_float=True)

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
        "purchase_value": 0.0
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
        export_filename = f"campaigns_{start_date}.json"

    # Pula dias vazios: não grava arquivo "[]" na raw (a validação exige não-vazio).
    if not data:
        print(f"⏭️  {start_date}: sem dados — nada gravado na raw")
        return data

    # Validação (staging in-task): todo registro não-vazio deve ter o id da entidade.
    if any("campaign_id" not in d for d in data):
        raise Exception(f"❌ Validação: registro sem campaign_id em {start_date}")

    write_raw(ENTITY, data, start_date, ING_DATE)
    return data

def get_action_value(actions, action_type, as_float=False):
    for action in actions:
        if action.get("action_type") == action_type:
            try:
                return float(action["value"]) if as_float else int(action["value"])
            except:
                return 0.0 if as_float else 0
    return 0.0 if as_float else 0

# ======================================
# 🔽 EXECUÇÃO
# ======================================
if __name__ == "__main__":
    # Janela = watermark(bronze) - lookback até D-1 (ou FB_LOAD_START/END p/ backfill).
    start_date, end_date = api_window(ENTITY)
    print(f"📅 Janela de ingestão (camp): {start_date} → {end_date}  [ingestion_date={ING_DATE}]")
    fetch_facebook_campaigns_by_day(
        start_date=start_date,
        end_date=end_date,
        only_with_data=True,
    )
