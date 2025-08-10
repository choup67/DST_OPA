import sys
from pathlib import Path
# ajout de la racine du projet au PYTHONPATH
sys.path.append(str(Path(__file__).resolve().parent.parent))
from UTILS.FONCTIONS.connexions import connect_to_snowflake
from UTILS.FONCTIONS.load_csv_to_stage import load_csv_to_stage

if __name__ == "__main__":
    print("Chargement des fichiers dans le stage OPA_STAGE")
    conn = connect_to_snowflake(schema="STAGE")
    try:
        load_csv_to_stage(conn, stage_name="OPA_STAGE") 
    finally:
        conn.close()
    print("Fichiers csv chargés dans le stage OPA_STAGE")