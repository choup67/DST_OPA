import os
from dotenv import load_dotenv
import snowflake.connector
from pathlib import Path

# Fonction pour trouver le fichier .env dans l'arborescence du projet
# Cette fonction est utilisée pour charger les variables d'environnement nécessaires à la connexion à Snowflake

def find_env_file(filename=".env", start_path=None):
    """
    Cherche le fichier .env en remontant l'arborescence,
    et à chaque niveau en explorant récursivement les sous-dossiers.
    """
    if start_path is None:
        start_path = Path(__file__).resolve()
    
    # On remonte du fichier jusqu'à la racine
    for parent in [start_path] + list(start_path.parents):
        for path in parent.rglob(filename):
            return path.resolve()
    
    raise FileNotFoundError(f"{filename} non trouvé à partir de {start_path}")

# Fonction pour établir une connexion à Snowflake
# Cette fonction est utilisée dans d'autres scripts pour éviter la duplication du code de connexion

def connect_to_snowflake(schema):
    """
    Établit une connexion à Snowflake après avoir trouvé et chargé le fichier .env
    où qu'il se trouve dans l'arborescence du projet.
    Args:
        schema (str): Le schéma Snowflake à utiliser pour la connexion.
    """
    env_path = find_env_file()
    load_dotenv(dotenv_path=env_path)

    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=schema,
        role=os.getenv('SNOWFLAKE_ROLE')
    )
