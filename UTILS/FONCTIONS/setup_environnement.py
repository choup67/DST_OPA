from pathlib import Path

def find_env_file(filename=".env", start_path=None):
    """
    Cherche le fichier .env en remontant l'arborescence,
    et à chaque niveau en explorant récursivement les sous-dossiers.
    """
    if start_path is None:
        start_path = Path(__file__).resolve()

    for parent in [start_path] + list(start_path.parents):
        for path in parent.rglob(filename):
            return path.resolve()

    raise FileNotFoundError(f"{filename} non trouvé à partir de {start_path}")