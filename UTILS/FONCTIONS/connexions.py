import os
from dotenv import load_dotenv
import snowflake.connector
from UTILS.FONCTIONS.setup_environnement import find_env_file

def connect_to_snowflake(schema: str):
    """
    Établit une connexion à Snowflake après avoir trouvé et chargé le fichier .env
    où qu'il se trouve dans l'arborescence du projet.

    Args:
        schema (str): Le schéma Snowflake à utiliser pour la connexion.

    Returns:
        snowflake.connector.connection.SnowflakeConnection: Objet de connexion à Snowflake
    """
    # Cherche et charge le fichier .env
    env_path = find_env_file()
    load_dotenv(dotenv_path=env_path)

    # Crée la connexion
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=schema,
        role=os.getenv("SNOWFLAKE_ROLE")
    )