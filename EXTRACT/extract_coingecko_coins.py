import time
import requests
import pandas as pd
from pathlib import Path

SAVE_PATH = Path(__file__).resolve().parent.parent / "UTILS" / "DATA" / "coingecko_top100_token_info.csv"
SAVE_PATH.parent.mkdir(parents=True, exist_ok=True)

BASE_DELAY = 6.0   # délai entre appels /coins/{id} (ajuste selon ta limite)
MAX_RETRIES = 5
TIMEOUT = 30

def get_top_100_tokens():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 100,
        "page": 1,
        "sparkline": False,
    }
    resp = requests.get(url, params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()

def get_token_metadata(coin_id, max_retries=MAX_RETRIES):
    delay = 2.0
    for attempt in range(max_retries):
        try:
            url = f"https://api.coingecko.com/api/v3/coins/{coin_id}"
            r = requests.get(url, timeout=TIMEOUT)
            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                wait = float(retry_after) if retry_after else min(60.0, delay)
                print(f"Rate limit pour {coin_id}, attente {wait}s")
                time.sleep(wait)
                delay *= 2
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            print(f"Erreur tentative {attempt + 1} pour {coin_id} : {e}")
            time.sleep(min(60.0, delay))
            delay *= 2
    return None

def extract_coingecko_top100():
    top_100 = get_top_100_tokens()
    rows = []

    for idx, token in enumerate(top_100, start=1):
        coin_id = token["id"]
        meta = get_token_metadata(coin_id)
        if meta is None:
            print(f"{idx}/100 : {coin_id} ignoré (erreur API)")
        else:
            rows.append({
                "coingecko_id": coin_id,
                "name": meta.get("name"),
                "symbol": (meta.get("symbol") or "").upper(),
                "market_cap_rank": meta.get("market_cap_rank"),
                "asset_platform": meta.get("asset_platform_id"),
                "token_type": meta.get("asset_platform_id") or "native",
                "homepage": (meta.get("links", {}).get("homepage") or [""])[0],
                "categories": ", ".join((meta.get("categories") or [])),
                "supply_circulating": meta.get("market_data", {}).get("circulating_supply"),
                "supply_total": meta.get("market_data", {}).get("total_supply"),
                "supply_max": meta.get("market_data", {}).get("max_supply"),
            })
            print(f"{idx}/100 : {coin_id} traité")

        time.sleep(BASE_DELAY)

    df = pd.DataFrame(rows)
    df.to_csv(SAVE_PATH, index=False)
    print(f"Fichier écrit : {SAVE_PATH}")

