from FONCTIONS.fonction_connexion import connect_to_snowflake
import snowflake.connector

# On remplace la vraie connexion par une simulation pour tester sans se connecter à Snowflake
def fake_connect(**kwargs):
    print("Connexion simulée avec :", kwargs)
    return "FAKE_CONNECTION"

snowflake.connector.connect = fake_connect

# Appel de la fonction
conn = connect_to_snowflake(schema="FAKE_SCHEMA")

print("Résultat de la connexion :", conn)