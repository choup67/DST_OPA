from pathlib import Path

def load_csv_to_stage(conn, stage_name = "OPA_STAGE", data_root = None, keep_tree = True):
    """
    Vide le stage Snowflake puis uploade tous les CSV trouvés dans UTILS/DATA.
    Retourne la liste des fichiers uploadés.
    """
    # Dossier DATA (…/UTILS/DATA), ce fichier étant dans …/UTILS/FONCTIONS
    if data_root is None:
        data_root = (Path(__file__).resolve().parent.parent / "DATA")
    data_root.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(data_root.rglob("*.csv"))
    if not csv_files:
        print(f"Aucun CSV trouvé dans {data_root}")
        return []

    cs = conn.cursor()
    try:
        cs.execute("USE DATABASE RAW_DATA")
        cs.execute("USE SCHEMA RAW_DATA.STAGE")
        cs.execute(f'CREATE STAGE IF NOT EXISTS "{stage_name}"')
        cs.execute(f"REMOVE @{stage_name}")
        print(f"Stage {stage_name} vidé.")

        uploaded = []
        for p in csv_files:
            local_uri = p.resolve().as_posix()

            if keep_tree:
                rel = p.relative_to(data_root).as_posix() 
                dest = f"@{stage_name}/{Path(rel).parent.as_posix()}" if "/" in rel else f"@{stage_name}"
            else:
                dest = f"@{stage_name}"

            put_sql = (
                f"PUT file://{local_uri} {dest} "
                f"AUTO_COMPRESS=TRUE OVERWRITE=TRUE PARALLEL=8"
            )
            cs.execute(put_sql)
            uploaded.append(p.name)

        print(f"Upload terminé. {len(uploaded)} fichiers envoyés dans @{stage_name}.")
        return uploaded
    finally:
        cs.close()