import requests
import pandas as pd

# Format d'affichage plus lisible pour les floats
#pd.options.display.float_format = '{:.8f}'.format
pd.options.display.float_format = lambda x: f'{x:.10f}'.rstrip('0').rstrip('.') if '.' in f'{x:.10f}' else f'{x:.10f}' # formatage des floats pour enlever les zéros inutiles

def get_pairs_list():
    url = 'https://api.binance.com/api/v3/exchangeInfo'
    data = requests.get(url).json()
    # Liste de toutes les paires
    pairs_list = data['symbols']
    return pairs_list

def get_pair_info(pair):
    filtres = { f['filterType']: f for f in pair['filters'] } # on transforme la liste de filtres en dictionnaire pour un accès plus facile

    return {
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
    }

pairs_list = get_pairs_list()
pairs_general_infos = [get_pair_info(pair) for pair in pairs_list]

# Conversion en DataFrame
df_pairs_general_infos = pd.DataFrame(pairs_general_infos)

# Conversion des colonnes numériques en float
df_pairs_general_infos[['minPrice', 'maxPrice', 'tickSize', 'minQty', 'maxQty', 'stepSize', 'minNotional', 'maxNotional']] = df_pairs_general_infos[['minPrice', 'maxPrice', 'tickSize', 'minQty', 'maxQty', 'stepSize', 'minNotional', 'maxNotional']].astype(float)

# Affichage
print(df_pairs_general_infos.info())