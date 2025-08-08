import requests
import pandas as pd
from time import sleep

#récupération de la liste des paires
df_list = pd.DataFrame(requests.get('https://api.binance.us/api/v3/ticker/price').json())
# Extraction des symboles de la liste de pairs
pairs_list = []
for i in range(len(df_list)):
    pairs_list.append(df_list['symbol'][i])

# Définition de la fonction pour récupérer les données des klines
# Note: intervalle de 1d pour une granularité quotidienne
def recup_klines(pair):
    url = f'https://api.binance.us/api/v3/klines?symbol={pair}&interval=1d&limit=1000'
    columns = [
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
    ]
    response = requests.get(url)
    data = pd.DataFrame(response.json(), columns=columns)
    data["symbol"] = pair
    return data

# Récupération des données pour chaque symbole
klines_data = pd.concat(
    [recup_klines(pair) for pair in pairs_list if not recup_klines(pair).empty],
    ignore_index=True
) if pairs_list else pd.DataFrame()

# Affichage des résultats
print(klines_data.head() if not klines_data.empty else "Aucune donnée récupérée.")