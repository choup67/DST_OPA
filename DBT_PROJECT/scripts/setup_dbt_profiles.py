import os

# Dossiers et chemins
profiles_dir = os.path.join("DBT_PROJECT", "profiles")
os.makedirs(profiles_dir, exist_ok=True)

profiles_yml = """
opa_profile:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: hbvyxoa-li66752
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: "{{ env_var('SNOWFLAKE_ROLE') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE') }}"
      database: DBT
      schema: STAGING
      threads: 4
      client_session_keep_alive: false
"""

# Écriture du fichier profiles.yml
with open(os.path.join(profiles_dir, "profiles.yml"), "w") as f:
    f.write(profiles_yml.strip())

print("✅ DBT profiles.yml généré avec des variables d'environnement.")

# Création du .env modèle
env_template = """
# Variables d'environnement pour DBT (à adapter pour chaque utilisateur)
DBT_PROFILES_DIR=./DBT_PROJECT/profiles
SNOWFLAKE_USER=ton_utilisateur
SNOWFLAKE_PASSWORD=ton_mot_de_passe
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
"""

with open(".env", "w") as f:
    f.write(env_template.strip())

print("✅ Fichier .env créé à la racine du projet.")
print("💡 Pour l’utiliser : `source .env` avant de lancer dbt.")