import snowflake.connector
from UTILS.FONCTIONS.connexions import connect_to_snowflake

def load_exchange_info(df):
    conn = connect_to_snowflake(schema="BINANCE")
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO exchange_info (
            symbol, base_asset, quote_asset, status,
            minPrice, maxPrice, tickSize,
            minQty, maxQty, stepSize,
            minNotional, maxNotional
        ) VALUES (
            %(symbol)s, %(baseAsset)s, %(quoteAsset)s, %(status)s,
            %(minPrice)s, %(maxPrice)s, %(tickSize)s,
            %(minQty)s, %(maxQty)s, %(stepSize)s,
            %(minNotional)s, %(maxNotional)s
        )
    """

    for _, row in df.iterrows():
        try:
            cursor.execute(insert_query, row.to_dict())
        except Exception as e:
            print(f"[ERREUR] Insertion échouée pour {row['symbol']}: {e}")
            continue

    conn.commit()
    cursor.close()
    conn.close()
    print("Données chargées dans exchange_info")