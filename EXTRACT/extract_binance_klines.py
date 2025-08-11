import requests
import pandas as pd
from time import sleep
from pathlib import Path

def extract_klines(interval="1d", limit=1000):
    """Récupère les données Klines pour toutes les paires Binance."""
    # Récupération directe des symboles
    pairs_list = [
        item["symbol"]
        for item in requests.get('https://api.binance.us/api/v3/ticker/price').json()
    ]
    
    columns = [
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
    ]
    
    all_data = []
    for pair in pairs_list:
        url = f'https://api.binance.us/api/v3/klines?symbol={pair}&interval={interval}&limit={limit}'
        resp = requests.get(url).json()
        if resp:  # Évite les paires vides
            df = pd.DataFrame(resp, columns=columns)
            df["symbol"] = pair
            all_data.append(df)
    
    klines_data = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

    # Chemin vers UTILS/DATA
    data_dir = Path(__file__).resolve().parent.parent / "UTILS" / "DATA"
    data_dir.mkdir(parents=True, exist_ok=True)

    csv_path = data_dir / "binance_klines.csv"
    klines_data.to_csv(csv_path, index=False, encoding='utf-8')
    print(f"CSV sauvegardé : {csv_path}")

    return klines_data
