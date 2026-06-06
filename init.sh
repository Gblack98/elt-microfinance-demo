#!/usr/bin/env bash
set -euo pipefail

echo "=== ELT Microfinance Demo — Setup ==="

# 1. Vérifier Python 3.10+
PY=$(python3 --version 2>&1 | grep -oP '3\.\K[0-9]+' || echo "0")
if [ "$PY" -lt 10 ]; then
  echo "Python 3.10+ requis (trouvé : $(python3 --version))"; exit 1
fi

# 2. Venv
if [ ! -d ".venv" ]; then
  echo "Création du venv..."
  python3 -m venv .venv
fi
source .venv/bin/activate
pip install -q --upgrade pip
pip install -q -r requirements.txt

# 3. Générer les CSV si absents
if [ ! -f "data/raw/loans.csv" ]; then
  echo "Génération des données synthétiques..."
  python3 scripts/generate_data.py
fi

# 4. Airflow
export AIRFLOW_HOME="$(pwd)/airflow_home"
export ELT_PROJECT_DIR="$(pwd)"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

mkdir -p airflow_home/dags
cp dags/elt_pipeline.py airflow_home/dags/

echo "Initialisation de la base Airflow..."
airflow db migrate

# Créer l'utilisateur admin si absent
airflow users list 2>/dev/null | grep -q admin || \
  airflow users create \
    --username admin --password admin \
    --firstname Admin --lastname User \
    --role Admin --email admin@example.com

# 5. Valider dbt
echo "Validation dbt..."
cd dbt_project && dbt debug --profiles-dir . 2>&1 | tail -5 && cd ..

echo ""
echo "=== Setup terminé ==="
echo "Démarrer le pipeline :"
echo "  Terminal 1 : source .venv/bin/activate && export AIRFLOW_HOME=\$(pwd)/airflow_home && export ELT_PROJECT_DIR=\$(pwd) && airflow scheduler"
echo "  Terminal 2 : source .venv/bin/activate && export AIRFLOW_HOME=\$(pwd)/airflow_home && airflow webserver -p 8080"
echo "  UI         : http://localhost:8080  (admin / admin)"
