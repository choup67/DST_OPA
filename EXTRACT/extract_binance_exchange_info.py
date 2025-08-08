import requests
import pandas as pd

def get_exchange_info():
    url = 'https://api.binance.com/api/v3/exchangeInfo'
    response = requests.get(url)
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
    
    return pd.DataFrame(data)

get_exchange_info_df = get_exchange_info()
print(get_exchange_info_df.head())