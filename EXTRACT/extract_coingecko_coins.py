import time
import requests
import pandas as pd
from pathlib import Path

SAVE_PATH = Path(__file__).resolve().parent.parent / "UTILS" / "DATA" / "coingecko_top100_token_info.csv"
SAVE_PATH.parent.mkdir(parents=True, exist_ok=True)

def get_top_100_tokens():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 100,
        "page": 1,
        "sparkline": False,
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def get_token_metadata_with_retry(coin_id, max_retries=5, delay=2.5):
    for attempt in range(max_retries):
        try:
            url = f"https://api.coingecko.com/api/v3/coins/{coin_id}"
            r = requests.get(url, timeout=30)
            if r.status_code == 429:
                print(f"Rate limit atteint pour {coin_id}, attente 60s.")
                time.sleep(60)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"Erreur tentative {attempt + 1} pour {coin_id} : {e}")
            time.sleep(delay)
    return None

def main():
    top_100 = get_top_100_tokens()
    rows = []

    for i, token in enumerate(top_100, 1):
        coin_id = token["id"]
        meta = get_token_metadata_with_retry(coin_id)
        if meta is None:
            continue

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

        if i % 10 == 0:
            print(f"{i}/100 tokens traités")
        time.sleep(2.5)

    df = pd.DataFrame(rows)
    df.to_csv(SAVE_PATH, index=False)
    print(f"Fichier écrit : {SAVE_PATH}")

if __name__ == "__main__":
    main()