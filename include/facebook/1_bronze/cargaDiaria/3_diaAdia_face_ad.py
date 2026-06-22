
import os
import re
import json
import time
import requests
from datetime import datetime, timedelta
from azure.storage.blob import ContentSettings

from facebook_config import get_blob_service_client, get_container_client, BRONZE as CONTAINER_NAME

# =========================
# Configuração básica
# =========================

# Facebook
ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")
AD_ACCOUNT_ID = os.getenv("FB_AD_ACCOUNT_ID")

# Azure (container datamartbi*, auto-criado)
DEST_FOLDER = "source_facebook/facebook_ad"
CARREGADOS_FOLDER = "source_facebook/facebook_ad_carregados"

get_container_client("bronze")  # garante que o container exista
blob_service_client = get_blob_service_client()

# =========================
# Utilidades de chamada ao Graph API
# =========================
def _api_get(url: str, params: dict, retries: int = 3, backoff: float = 1.5):
    """
    GET com tentativas simples e backoff para lidar com 5xx e limite de rate.
    """
    for i in range(retries):
        r = requests.get(url, params=params)
        if r.status_code == 200:
            return r.json()
        # Tratamento básico para rate limit / erros transitórios
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(backoff * (i + 1))
            continue
        # Outros erros: falha direta
        raise Exception(f"Erro API Facebook: {r.status_code} - {r.text}")
    # Se esgotar tentativas
    r.raise_for_status()

def _get_ad_creative(ad_id: str):
    """
    Busca o 'creative' do anúncio para localizar video_id ou object_story_id.
    """
    url = f"https://graph.facebook.com/v19.0/{ad_id}"
    params = {
        "access_token": ACCESS_TOKEN,
        "fields": "creative{effective_object_story_id,object_story_id,object_type,asset_feed_spec,object_story_spec}"
    }
    data = _api_get(url, params)
    return ((data or {}).get("creative") or {})

def _get_video_meta(video_id: str):
    """
    Retorna dict com permalink_url e thumbnail para o video_id.
    """
    url = f"https://graph.facebook.com/v19.0/{video_id}"
    params = {
        "access_token": ACCESS_TOKEN,
        "fields": "permalink_url,picture,thumbnails{uri}"
    }
    data = _api_get(url, params) or {}
    thumb = data.get("picture", "")
    thumbs = (data.get("thumbnails") or {}).get("data") or []
    if not thumb and thumbs:
        thumb = thumbs[0].get("uri", "")
    return {
        "permalink_url": data.get("permalink_url", "") or "",
        "thumbnail": thumb or ""
    }

def _get_posts_meta_batch(post_ids: list[str]) -> dict:
    """
    Resolve vários pageid_postid em uma única chamada:
    retorna dict[id] = {"permalink_url": ..., "thumbnail": ...}
    """
    if not post_ids:
        return {}
    # Remover duplicados e limitar fatias para evitar URLs muito grandes
    unique_ids = list(dict.fromkeys(post_ids))
    results = {}
    chunk_size = 50  # tamanho de lote seguro
    for i in range(0, len(unique_ids), chunk_size):
        chunk = unique_ids[i:i+chunk_size]
        url = "https://graph.facebook.com/v19.0"
        params = {
            "access_token": ACCESS_TOKEN,
            "ids": ",".join(chunk),
            "fields": "permalink_url,full_picture"
        }
        try:
            data = _api_get(url, params) or {}
            for pid in chunk:
                d = data.get(pid) or {}
                results[pid] = {
                    "permalink_url": d.get("permalink_url", "") or "",
                    "thumbnail": d.get("full_picture", "") or ""
                }
        except Exception:
            # Em caso de falha no lote, marca todos como não resolvidos
            for pid in chunk:
                results[pid] = {"permalink_url": "", "thumbnail": ""}
    return results

# =========================
# Normalização de URLs
# =========================
def _normalize_video_url(url: str) -> str:
    """
    Converte retornos relativos/IDs em URL absoluta clicável.
    Trata também padrão pageid_postid (^\d+_\d+$).
    """
    if not url:
        return ""

    if url.startswith(("http://", "https://")):
        return url

    # /videos/<id> -> watch?v=<id>
    m = re.search(r"/videos/(\d+)", url)
    if m:
        vid = m.group(1)
        return f"https://www.facebook.com/watch/?v={vid}"

    # Somente dígitos (video_id)
    if url.isdigit():
        return f"https://www.facebook.com/watch/?v={url}"

    # pageid_postid
    m2 = re.fullmatch(r"(\d+)_(\d+)", url)
    if m2:
        page_id, post_id = m2.groups()
        return f"https://www.facebook.com/{page_id}/posts/{post_id}"

    # relativo
    if url.startswith("/"):
        return "https://www.facebook.com" + url

    return url

def _to_click_url(url: str) -> str:
    """
    Melhor URL para clique: se houver /videos/<id>, força watch/?v=<id>.
    Senão, retorna normalizada.
    """
    if not url:
        return ""
    m = re.search(r"/videos/(\d+)", url)
    if m:
        return f"https://www.facebook.com/watch/?v={m.group(1)}"
    return _normalize_video_url(url)

# =========================
# Extração de metadados de vídeo/post
# =========================
def _extract_video_meta_from_creative(creative: dict):
    """
    Ordem:
      1) object_story_spec.video_data.video_id -> meta de vídeo
      2) asset_feed_spec.videos[].video_id -> meta de vídeo
      3) effective_object_story_id / object_story_id -> devolver ID para
         resolução posterior em lote (pageid_postid).
    Retorna tuple:
      (video_url, video_thumbnail, pending_post_id)
    Onde:
      - video_url já vem como permalink quando for video_id,
        ou string vazia quando depender de post.
      - pending_post_id: string 'pageid_postid' que precisa ser resolvida depois.
    """
    if not creative:
        return "", "", None

    # 1) video_data.video_id
    oss = creative.get("object_story_spec") or {}
    video_data = oss.get("video_data") or {}
    vid = video_data.get("video_id")
    if vid:
        try:
            vm = _get_video_meta(str(vid))
            url = vm.get("permalink_url", "") or f"https://www.facebook.com/watch/?v={vid}"
            return url, vm.get("thumbnail", ""), None
        except Exception:
            return f"https://www.facebook.com/watch/?v={vid}", "", None

    # 2) asset_feed_spec.videos[]
    afs = creative.get("asset_feed_spec") or {}
    for v in (afs.get("videos") or []):
        vid2 = v.get("video_id")
        if vid2:
            try:
                vm2 = _get_video_meta(str(vid2))
                url = vm2.get("permalink_url", "") or f"https://www.facebook.com/watch/?v={vid2}"
                return url, vm2.get("thumbnail", ""), None
            except Exception:
                return f"https://www.facebook.com/watch/?v={vid2}", "", None

    # 3) post fallback (effective/object_story_id ou object_story_id)
    for key in ("effective_object_story_id", "object_story_id"):
        pid = creative.get(key)
        if pid:
            return "", "", str(pid)

    return "", "", None

# =========================
# Lógica de extração e envio
# =========================
def get_latest_loaded_date():
    """
    Lê a pasta 'carregados' para descobrir a última data processada
    no formato de arquivo 'ads_YYYY-MM-DD.json'.
    """
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    blobs = container_client.list_blobs(name_starts_with=CARREGADOS_FOLDER + "/")

    datas = []
    for blob in blobs:
        nome = os.path.basename(blob.name)
        if nome.startswith("ads_") and nome.endswith(".json"):
            try:
                data_str = nome.replace("ads_", "").replace(".json", "")
                data = datetime.strptime(data_str, "%Y-%m-%d")
                datas.append(data)
            except Exception:
                continue

    if not datas:
        raise Exception("Nenhum arquivo encontrado na pasta carregados.")
    return max(datas)

def fetch_facebook_ads_by_day(start_date, end_date, only_with_data=False):
    """
    Itera dia a dia e grava um arquivo JSON por data.
    """
    print("Gerando arquivos por dia (Ads)...")

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    delta = timedelta(days=1)

    while start <= end:
        date_str = start.strftime("%Y-%m-%d")
        print(f"Processando {date_str}...")

        data = fetch_facebook_ads_direct(
            start_date=date_str,
            end_date=date_str,
            only_with_data=only_with_data,
            export_filename=f"ads_{date_str}.json"
        )

        print(f"{date_str}: {len(data)} registros")
        start += delta

def fetch_facebook_ads_direct(start_date, end_date, only_with_data=False, export_filename=None):
    """
    Consulta /insights em nível de anúncio, anexa métricas e
    resolve URLs de vídeo/post.
    """
    required_fields = [
        "ad_id", "ad_name", "campaign_id", "adset_id",
        "date_start", "date_stop", "spend", "impressions", "clicks",
        "ctr", "cpc", "reach", "frequency", "cost_per_unique_click",
        "actions"
    ]

    url = f"https://graph.facebook.com/v19.0/{AD_ACCOUNT_ID}/insights"
    params = {
        "access_token": ACCESS_TOKEN,
        "time_range": json.dumps({"since": start_date, "until": end_date}),
        "level": "ad",
        "fields": ",".join(required_fields),
        "limit": 500
    }

    # Paginação
    all_rows = []
    while True:
        resp = _api_get(url, params)
        rows = resp.get("data", []) or []
        all_rows.extend(rows)
        paging = resp.get("paging", {})
        next_url = (paging.get("next") or "")
        if not next_url:
            break
        # Quando vem 'next', a melhor forma é seguir a URL completa
        # e limpar params para evitar sobrescrever
        url = next_url
        params = {}

    data = all_rows

    # Cache por ad_id
    ad_ids = {str(item.get("ad_id")) for item in data if item.get("ad_id")}
    # Armazenam metadados resolvidos por ad_id
    video_meta_cache = {}
    # Post IDs pendentes para resolução em lote
    pending_post_ids = set()

    # 1) Descobrir creativos e coletar video_id ou post_id
    for ad_id in ad_ids:
        try:
            creative = _get_ad_creative(ad_id)
            url_found, thumb_found, pending_post = _extract_video_meta_from_creative(creative)

            if pending_post:
                pending_post_ids.add(pending_post)
                video_meta_cache[ad_id] = {
                    "video_url": "",           # preencheremos depois com permalink do post
                    "video_url_click": "",
                    "video_thumbnail_url": ""
                }
            else:
                # Já veio resolvido por video_id
                normalized = _normalize_video_url(url_found)
                click_url = _to_click_url(normalized)
                video_meta_cache[ad_id] = {
                    "video_url": normalized,
                    "video_url_click": click_url,
                    "video_thumbnail_url": thumb_found or ""
                }
        except Exception:
            video_meta_cache[ad_id] = {
                "video_url": "",
                "video_url_click": "",
                "video_thumbnail_url": ""
            }

    # 2) Resolver posts pendentes em lote
    if pending_post_ids:
        post_meta_map = _get_posts_meta_batch(list(pending_post_ids))
    else:
        post_meta_map = {}

    # 3) Enriquecer cada linha
    for item in data:
        actions = item.pop("actions", []) or []
        item["leads"] = get_action_value(actions, "lead")
        item["purchases"] = get_action_value(actions, "purchase")
        item["purchase_value"] = get_action_value(actions, "omni_purchase", as_float=True)

        aid = str(item.get("ad_id")) if item.get("ad_id") is not None else ""
        meta = video_meta_cache.get(aid, {})

        # Se ainda não houver video_url (caso de post), tente achar o post no creative
        if not meta.get("video_url"):
            try:
                creative = _get_ad_creative(aid)
                pid = None
                for key in ("effective_object_story_id", "object_story_id"):
                    if creative.get(key):
                        pid = str(creative[key])
                        break
                if pid:
                    pmeta = post_meta_map.get(pid, {"permalink_url": "", "thumbnail": ""})
                    permalink = pmeta.get("permalink_url", "")
                    thumb = pmeta.get("thumbnail", "")
                    if not permalink:
                        # Fallback para URL normalizada
                        permalink = _normalize_video_url(pid)
                    meta = {
                        "video_url": permalink or "",
                        "video_url_click": _to_click_url(permalink) if permalink else "",
                        "video_thumbnail_url": thumb or ""
                    }
            except Exception:
                pass

        # Gravar nos campos finais
        item["video_url"] = meta.get("video_url", "") or ""
        item["video_url_click"] = meta.get("video_url_click", "") or ""
        item["video_thumbnail_url"] = meta.get("video_thumbnail_url", "") or ""

    # 4) Garantir campos padrão
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
        "video_url": "",
        "video_url_click": "",
        "video_thumbnail_url": ""
    }
    for item in data:
        for campo, valor_padrao in campos_padrao.items():
            if campo not in item:
                item[campo] = valor_padrao

    # 5) Filtrar somente com dados (opcional)
    if only_with_data:
        data = [d for d in data if any([
            float(d.get("spend", 0) or 0) > 0,
            int(float(d.get("impressions", 0) or 0)) > 0,
            int(float(d.get("clicks", 0) or 0)) > 0,
            int(float(d.get("leads", 0) or 0)) > 0,
            int(float(d.get("purchases", 0) or 0)) > 0,
            float(d.get("purchase_value", 0) or 0) > 0
        ])]

    # 6) Upload para Azure
    if not export_filename:
        export_filename = f"ads_{start_date}.json"

    json_str = json.dumps(data, indent=2, ensure_ascii=False)
    blob_path = f"{DEST_FOLDER}/{export_filename}"
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_path)

    blob_client.upload_blob(
        json_str.encode("utf-8"),
        overwrite=True,
        content_settings=ContentSettings(content_type="application/json")
    )
    print(f"Enviado para Azure: {blob_path}")

    return data

def get_action_value(actions, action_type, as_float=False):
    """
    Lê valor de 'actions' tratando string/float/int.
    """
    for action in actions:
        if action.get("action_type") == action_type:
            try:
                val = action.get("value", 0)
                return float(val) if as_float else int(float(val))
            except Exception:
                return 0.0 if as_float else 0
    return 0.0 if as_float else 0

# =========================
# Execução
# =========================
if __name__ == "__main__":
    # Backfill por intervalo via env (ex.: carga de 2 meses). Se FB_LOAD_START_DATE
    # e FB_LOAD_END_DATE estiverem setadas, usa esse intervalo; senão, incremental.
    _start_env = os.getenv("FB_LOAD_START_DATE")
    _end_env = os.getenv("FB_LOAD_END_DATE")

    if _start_env and _end_env:
        print(f"📅 Carga por intervalo (env): {_start_env} → {_end_env}")
        fetch_facebook_ads_by_day(
            start_date=_start_env,
            end_date=_end_env,
            only_with_data=True
        )
    else:
        latest_loaded = get_latest_loaded_date()
        ontem = datetime.now() - timedelta(days=1)

        if latest_loaded.date() >= ontem.date():
            print("Dados já atualizados até ontem. Nada a fazer.")
        else:
            start_date = (latest_loaded + timedelta(days=1)).strftime("%Y-%m-%d")
            end_date = ontem.strftime("%Y-%m-%d")
            print(f"Iniciando extração de {start_date} até {end_date}...")
            fetch_facebook_ads_by_day(
                start_date=start_date,
                end_date=end_date,
                only_with_data=True
            )

