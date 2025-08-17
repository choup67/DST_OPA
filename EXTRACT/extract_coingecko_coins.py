import time
import requests
import pandas as pd
from pathlib import Path

SAVE_PATH = Path(__file__).resolve().parent.parent / "UTILS" / "DATA" / "coingecko_top1000_token_info.csv"
SAVE_PATH.parent.mkdir(parents=True, exist_ok=True)

BASE_DELAY = 6.0
MAX_RETRIES = 5
TIMEOUT = 30

def get_top_1000_tokens():
    # -> récupère ~1000 tokens via 4 pages de 250
    url = "https://api.coingecko.com/api/v3/coins/markets"
    all_rows = []
    for page in range(1, 5):  # 4 pages x 250 = 1000
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 250,
            "page": page,
            "sparkline": False,
        }
        resp = requests.get(url, params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        all_rows.extend(data)
        time.sleep(1.0) 
    return all_rows

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

def extract_coingecko_top1000():
    top_tokens = get_top_1000_tokens()
    total = len(top_tokens)
    rows = []

    for idx, token in enumerate(top_tokens, start=1):
        coin_id = token["id"]
        meta = get_token_metadata(coin_id)
        if meta is None:
            print(f"{idx}/{total} : {coin_id} ignoré (erreur API)")
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
            print(f"{idx}/{total} : {coin_id} traité")

        time.sleep(BASE_DELAY)

    df = pd.DataFrame(rows)
    df.to_csv(SAVE_PATH, index=False)
    print(f"Fichier écrit : {SAVE_PATH}")

extract_coingecko_top1000()