# Setup initial
git clone git@github.com:ton-user/OPA.git
cd OPA
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Configuration locale
cp .env.example .env  # ou créer manuellement .env
# ➤ Editer .env avec ses identifiants Snowflake

# Lancer le projet
source .env
dbt debug