import requests
import pandas as pd
from pathlib import Path

def get_exchange_info():
    url = 'https://api.binance.com/api/v3/exchangeInfo'
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    
    pairs = response.json().get('symbols', [])
    data = []
    
    for pair in pairs:
        filtres = {f['filterType']: f for f in pair['filters']}
        data.append({
            'symbol': pair['symbol'],
            'baseAsset': pair['baseAsset'],
            'quoteAsset': pair['quoteAsset'],
            'status': pair['status'],
            'minPrice': filtres.get('PRICE_FILTER', {}).get('minPrice'),
            'maxPrice': filtres.get('PRICE_FILTER', {}).get('maxPrice'),
            'tickSize': filtres.get('PRICE_FILTER', {}).get('tickSize'),
            'minQty': filtres.get('LOT_SIZE', {}).get('minQty'),
            'maxQty': filtres.get('LOT_SIZE', {}).get('maxQty'),
            'stepSize': filtres.get('LOT_SIZE', {}).get('stepSize'),
            'minNotional': filtres.get('NOTIONAL', {}).get('minNotional'),
            'maxNotional': filtres.get('NOTIONAL', {}).get('maxNotional'),
        })
    
    df = pd.DataFrame(data)

    # Chemin vers UTILS/DATA
    data_dir = Path(__file__).resolve().parent.parent / "UTILS" / "DATA"
    data_dir.mkdir(parents=True, exist_ok=True)

    csv_path = data_dir / "binance_exchange_info.csv"
    df.to_csv(csv_path, index=False, encoding='utf-8')
    print(f"CSV sauvegardé : {csv_path}")

    return df
