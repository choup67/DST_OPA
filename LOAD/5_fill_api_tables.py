import sys
from pathlib import Path
# Ajouter la racine du projet au PYTHONPATH
sys.path.append(str(Path(__file__).resolve().parent.parent))

from EXTRACT.extract_binance_exchange_info import get_exchange_info
from LOAD.load_binance_exchange_info import load_exchange_info
from UTILS.FONCTIONS.connexions import connect_to_snowflake

if __name__ == "__main__":
    print("Début pipeline Binance Exchange Info")
    df = get_exchange_info()
    load_exchange_info(df)
    print("Fin pipeline Binance Exchange Info")