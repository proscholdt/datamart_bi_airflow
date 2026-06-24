"""Ingestão facebook — CAMPANHA (API Graph → STAGE).

Insights nível campaign com time_increment=1 (poucas chamadas, fatiado p/ evitar
timeout), 1 arquivo JSON por dia no stage. Substitui o loop dia-a-dia (1 chamada/
dia) — no tier `development_access` o dia-a-dia estoura o rate limit. Janela vem do
api_window (incremental: watermark do bronze − lookback até D-1; ou FB_LOAD_* p/ backfill).
"""
import os
import json
import time
import datetime as _dt

import requests

from facebook_config import get_blob_service_client, get_container_client
from fb_meta import api_window
from to_stage.stage_io import write_stage

ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")
AD_ACCOUNT_ID = os.getenv("FB_AD_ACCOUNT_ID")
ENTITY = "camp"
G = "https://graph.facebook.com/v19.0"

get_container_client("stage")  # zona de pouso
blob_service_client = get_blob_service_client()

REQUIRED_FIELDS = [
    "campaign_id", "campaign_name", "date_start", "date_stop", "spend", "impressions",
    "clicks", "ctr", "cpc", "reach", "frequency", "cost_per_unique_click", "actions",
]
DEFAULTS = {
    "spend": 0.0, "impressions": 0, "clicks": 0, "ctr": 0.0, "cpc": 0.0, "reach": 0,
    "frequency": 0.0, "cost_per_unique_click": 0.0, "leads": 0, "purchases": 0, "purchase_value": 0.0,
}


def _get(url, params, tag=""):
    """GET com retry: rate limit (code17) dorme 90s; transitório/timeout (code2/100/5xx/429) 20s."""
    for attempt in range(10):
        try:
            r = requests.get(url, params=params, timeout=180)
        except requests.RequestException as e:
            print(f"  [net {tag}] {type(e).__name__} 15s ({attempt+1})"); time.sleep(15); continue
        if r.status_code == 200:
            return r.json()
        txt = r.text; low = txt.lower()
        if '"code":17' in txt or "request limit" in low:
            print(f"  [rate {tag}] 90s ({attempt+1})"); time.sleep(90); continue
        if (r.status_code in (429, 500, 502, 503, 504) or '"code":2' in txt or '"code":100' in txt
                or "temporarily unavailable" in low or "tente novamente" in low or "expirou" in low or "try again" in low):
            print(f"  [transient {tag}] 20s ({attempt+1})"); time.sleep(20); continue
        raise Exception(f"Erro API Facebook ({tag}): {r.status_code} - {txt[:300]}")
    raise Exception(f"Falha persistente em {tag}")


def _chunks(start, end, size=14):
    """Janelas de ~14 dias (insights pesado estoura timeout em ranges longos)."""
    a = _dt.date.fromisoformat(start); b = _dt.date.fromisoformat(end); out = []
    while a <= b:
        c = min(a + _dt.timedelta(days=size - 1), b)
        out.append((a.isoformat(), c.isoformat())); a = c + _dt.timedelta(days=1)
    return out


def get_action_value(actions, action_type, as_float=False):
    for action in actions or []:
        if action.get("action_type") == action_type:
            try:
                return float(action["value"]) if as_float else int(float(action["value"]))
            except Exception:
                return 0.0 if as_float else 0
    return 0.0 if as_float else 0


def fetch_facebook_campaigns_by_day(start_date, end_date, only_with_data=False):
    rows = []
    for cs, ce in _chunks(start_date, end_date):
        u = f"{G}/{AD_ACCOUNT_ID}/insights"
        p = {"access_token": ACCESS_TOKEN, "time_range": json.dumps({"since": cs, "until": ce}),
             "time_increment": 1, "level": "campaign", "fields": ",".join(REQUIRED_FIELDS), "limit": 500}
        while True:
            j = _get(u, p, f"insights[{cs}]"); rows += j.get("data", [])
            nxt = j.get("paging", {}).get("next")
            if not nxt:
                break
            u, p = nxt, {}
    print(f"insights camp: {len(rows)} linhas ({start_date}→{end_date})")

    by_day = {}
    for it in rows:
        by_day.setdefault(it.get("date_start"), []).append(it)

    for date_str in sorted(by_day):
        items = by_day[date_str]
        for item in items:
            actions = item.pop("actions", [])
            item["leads"] = get_action_value(actions, "lead")
            item["purchases"] = get_action_value(actions, "purchase")
            item["purchase_value"] = get_action_value(actions, "omni_purchase", as_float=True)
            for campo, val in DEFAULTS.items():
                if campo not in item:
                    item[campo] = val
        data = items
        if only_with_data:
            data = [d for d in data if any([
                float(d.get("spend", 0)) > 0, int(d.get("impressions", 0)) > 0,
                int(d.get("clicks", 0)) > 0, int(d.get("leads", 0)) > 0,
                int(d.get("purchases", 0)) > 0, float(d.get("purchase_value", 0)) > 0,
            ])]
        if not data:
            print(f"⏭️  {date_str}: sem dados")
            continue
        if any("campaign_id" not in d for d in data):
            raise Exception(f"❌ Validação: registro sem campaign_id em {date_str}")
        write_stage(ENTITY, data, date_str)
        print(f"{date_str}: {len(data)} registros")


if __name__ == "__main__":
    start_date, end_date = api_window(ENTITY)
    print(f"📅 Janela de ingestão (camp): {start_date} → {end_date}")
    fetch_facebook_campaigns_by_day(start_date, end_date, only_with_data=True)
