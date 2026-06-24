"""Ingestão facebook — AD (API Graph → STAGE).

Insights nível ad com time_increment=1 (fatiado p/ evitar timeout) + resolução de
criativo/vídeo EM LOTE (parâmetro `ids`, 1× por anúncio — não por dia). O método
antigo resolvia o criativo por dia por anúncio (centenas/milhares de chamadas →
rate limit no tier development_access); este faz ~dezenas. 1 JSON/dia no stage.
Janela do api_window (incremental: watermark − lookback até D-1; ou FB_LOAD_* p/ backfill).
"""
import os
import re
import json
import time
import datetime as _dt

import requests

from facebook_config import get_blob_service_client, get_container_client
from fb_meta import api_window
from to_stage.stage_io import write_stage

ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")
AD_ACCOUNT_ID = os.getenv("FB_AD_ACCOUNT_ID")
ENTITY = "ad"
G = "https://graph.facebook.com/v19.0"

get_container_client("stage")
blob_service_client = get_blob_service_client()

REQUIRED_FIELDS = [
    "ad_id", "ad_name", "campaign_id", "adset_id", "date_start", "date_stop",
    "spend", "impressions", "clicks", "ctr", "cpc", "reach", "frequency", "cost_per_unique_click", "actions",
]
DEFAULTS = {
    "spend": 0.0, "impressions": 0, "clicks": 0, "ctr": 0.0, "cpc": 0.0, "reach": 0,
    "frequency": 0.0, "cost_per_unique_click": 0.0, "leads": 0, "purchases": 0, "purchase_value": 0.0,
    "video_url": "", "video_url_click": "", "video_thumbnail_url": "",
}
CREATIVE_FIELDS = "creative{effective_object_story_id,object_story_id,object_type,asset_feed_spec,object_story_spec}"


def _get(url, params, tag=""):
    """GET com retry: rate limit (code17) 90s; transitório/timeout (code2/100/5xx/429) 20s."""
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
    a = _dt.date.fromisoformat(start); b = _dt.date.fromisoformat(end); out = []
    while a <= b:
        c = min(a + _dt.timedelta(days=size - 1), b)
        out.append((a.isoformat(), c.isoformat())); a = c + _dt.timedelta(days=1)
    return out


def _batched(ids, fields, tag):
    """GET /?ids=...&fields=... em lotes de 50 → dict[id]=obj.

    Tolerante: _get já re-tenta rate limit/transitório; se um lote falhar por erro
    PERMANENTE (ex.: posts exigem 'pages_read_engagement', code #10), segue sem
    aqueles ids — o vídeo cai no fallback (URL construída do pageid_postid).
    """
    out = {}
    ids = list(dict.fromkeys(ids))
    for i in range(0, len(ids), 50):
        try:
            j = _get(G, {"access_token": ACCESS_TOKEN, "ids": ",".join(ids[i:i + 50]), "fields": fields}, f"{tag}[{i}]")
            out.update(j)
        except Exception as e:
            print(f"  [skip {tag}] lote ignorado ({type(e).__name__}: {str(e)[:120]})")
    return out


def get_action_value(actions, action_type, as_float=False):
    for action in actions or []:
        if action.get("action_type") == action_type:
            try:
                val = action.get("value", 0)
                return float(val) if as_float else int(float(val))
            except Exception:
                return 0.0 if as_float else 0
    return 0.0 if as_float else 0


# --- normalização de URL de vídeo/post (idêntica ao método antigo) ---------- #
def _normalize_video_url(url):
    if not url:
        return ""
    if url.startswith(("http://", "https://")):
        return url
    m = re.search(r"/videos/(\d+)", url)
    if m:
        return f"https://www.facebook.com/watch/?v={m.group(1)}"
    if url.isdigit():
        return f"https://www.facebook.com/watch/?v={url}"
    m2 = re.fullmatch(r"(\d+)_(\d+)", url)
    if m2:
        return f"https://www.facebook.com/{m2.group(1)}/posts/{m2.group(2)}"
    if url.startswith("/"):
        return "https://www.facebook.com" + url
    return url


def _to_click_url(url):
    if not url:
        return ""
    m = re.search(r"/videos/(\d+)", url)
    if m:
        return f"https://www.facebook.com/watch/?v={m.group(1)}"
    return _normalize_video_url(url)


def _pick_video_id(cr):
    vid = ((cr.get("object_story_spec") or {}).get("video_data") or {}).get("video_id")
    if vid:
        return str(vid)
    for v in ((cr.get("asset_feed_spec") or {}).get("videos") or []):
        if v.get("video_id"):
            return str(v["video_id"])
    return None


def _pick_post_id(cr):
    for k in ("effective_object_story_id", "object_story_id"):
        if cr.get(k):
            return str(cr[k])
    return None


def _video_thumb(d):
    t = d.get("picture", "") or ""
    if not t:
        ths = (d.get("thumbnails") or {}).get("data") or []
        if ths:
            t = ths[0].get("uri", "") or ""
    return t


def _resolve_video(cr, video_meta, post_meta):
    """(video_url, video_url_click, video_thumbnail_url) — vídeo primeiro, senão post."""
    if not cr:
        return "", "", ""
    vid = _pick_video_id(cr)
    if vid:
        vm = video_meta.get(vid, {})
        url = vm.get("permalink_url", "") or f"https://www.facebook.com/watch/?v={vid}"
        nurl = _normalize_video_url(url)
        return nurl, _to_click_url(nurl), vm.get("thumbnail", "")
    pid = _pick_post_id(cr)
    if pid:
        pm = post_meta.get(pid, {})
        permalink = pm.get("permalink_url", "") or _normalize_video_url(pid)
        return permalink or "", (_to_click_url(permalink) if permalink else ""), pm.get("thumbnail", "") or ""
    return "", "", ""


def _resolve_video_map(ad_ids):
    """ad_id -> (video_url, click, thumbnail), resolvendo criativo/vídeo/post em lote."""
    creatives_raw = _batched(ad_ids, CREATIVE_FIELDS, "creative")
    creative_by_ad = {aid: (creatives_raw.get(aid, {}) or {}).get("creative") or {} for aid in ad_ids}

    vid_ids, post_ids = [], []
    for cr in creative_by_ad.values():
        v = _pick_video_id(cr)
        if v:
            vid_ids.append(v)
        else:
            pid = _pick_post_id(cr)
            if pid:
                post_ids.append(pid)

    vmeta_raw = _batched(vid_ids, "permalink_url,picture,thumbnails{uri}", "video") if vid_ids else {}
    video_meta = {vid: {"permalink_url": d.get("permalink_url", "") or "", "thumbnail": _video_thumb(d)}
                  for vid, d in vmeta_raw.items()}
    pmeta_raw = _batched(post_ids, "permalink_url,full_picture", "post") if post_ids else {}
    post_meta = {pid: {"permalink_url": d.get("permalink_url", "") or "", "thumbnail": d.get("full_picture", "") or ""}
                 for pid, d in pmeta_raw.items()}

    return {aid: _resolve_video(cr, video_meta, post_meta) for aid, cr in creative_by_ad.items()}


def fetch_facebook_ads_by_day(start_date, end_date, only_with_data=False):
    # 1) insights (fatiado p/ não estourar timeout)
    rows = []
    for cs, ce in _chunks(start_date, end_date):
        u = f"{G}/{AD_ACCOUNT_ID}/insights"
        p = {"access_token": ACCESS_TOKEN, "time_range": json.dumps({"since": cs, "until": ce}),
             "time_increment": 1, "level": "ad", "fields": ",".join(REQUIRED_FIELDS), "limit": 500}
        while True:
            j = _get(u, p, f"insights[{cs}]"); rows += j.get("data", [])
            nxt = j.get("paging", {}).get("next")
            if not nxt:
                break
            u, p = nxt, {}
    print(f"insights ad: {len(rows)} linhas ({start_date}→{end_date})")

    # 2) resolução de vídeo 1× por anúncio (em lote)
    uniq = sorted({str(r["ad_id"]) for r in rows if r.get("ad_id")})
    vmap = _resolve_video_map(uniq) if uniq else {}
    print(f"video_url resolvido: {sum(1 for x in vmap.values() if x[0])}/{len(uniq)}")

    # 3) agrupa por dia, processa (mesmo formato) e grava no stage
    by_day = {}
    for it in rows:
        by_day.setdefault(it.get("date_start"), []).append(it)

    for date_str in sorted(by_day):
        items = by_day[date_str]
        for item in items:
            actions = item.pop("actions", []) or []
            item["leads"] = get_action_value(actions, "lead")
            item["purchases"] = get_action_value(actions, "purchase")
            item["purchase_value"] = get_action_value(actions, "omni_purchase", as_float=True)
            vu, vc, vt = vmap.get(str(item.get("ad_id")), ("", "", ""))
            item["video_url"] = vu
            item["video_url_click"] = vc
            item["video_thumbnail_url"] = vt
            for campo, val in DEFAULTS.items():
                if campo not in item:
                    item[campo] = val
        data = items
        if only_with_data:
            data = [d for d in data if any([
                float(d.get("spend", 0) or 0) > 0, int(float(d.get("impressions", 0) or 0)) > 0,
                int(float(d.get("clicks", 0) or 0)) > 0, int(float(d.get("leads", 0) or 0)) > 0,
                int(float(d.get("purchases", 0) or 0)) > 0, float(d.get("purchase_value", 0) or 0) > 0,
            ])]
        if not data:
            print(f"⏭️  {date_str}: sem dados")
            continue
        if any("ad_id" not in d for d in data):
            raise Exception(f"❌ Validação: registro sem ad_id em {date_str}")
        write_stage(ENTITY, data, date_str)
        print(f"{date_str}: {len(data)} registros")


if __name__ == "__main__":
    start_date, end_date = api_window(ENTITY)
    print(f"📅 Janela de ingestão (ad): {start_date} → {end_date}")
    fetch_facebook_ads_by_day(start_date, end_date, only_with_data=True)
