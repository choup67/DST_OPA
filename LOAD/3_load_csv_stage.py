import os
from dotenv import load_dotenv
import snowflake.connector
from pathlib import Path

# Charger le .env depuis le dossier DBT_PROJECT
env_path = Path(__file__).parent.parent / 'DBT_PROJECT' / '.env'
load_dotenv(dotenv_path=env_path)

conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account='HBVYXOA-LI66752',
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database='RAW_DATA',
    schema='GITREPO',
    role=os.getenv('SNOWFLAKE_ROLE')
)

try:
    cs = conn.cursor()

    folder_path = "C:/Users/rivie/OneDrive/Bureau/ETL_dev-DataScientest/PROJET/DST_OPA/data"
    stage_name = 'my_stage'

    # Liste manuelle des fichiers à uploader
    files = [
        'table_fiat.csv',
        'market_feeling_score.csv'
    ]

    for file_name in files:
        local_file_path = os.path.join(folder_path, file_name)
        # Remplacer les backslashes par des slashs
        local_file_path = local_file_path.replace('\\', '/')
        put_command = f"PUT file://{local_file_path} @{stage_name} AUTO_COMPRESS=TRUE"
        print(f"Uploading {file_name} to stage {stage_name}...")
        cs.execute(put_command)
        print(f"Upload de {file_name} terminé.")

finally:
    cs.close()
    conn.close()