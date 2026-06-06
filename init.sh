#!/usr/bin/env bash
set -euo pipefail

echo "=== ELT Microfinance Demo — Setup ==="

# 1. Vérifier Python 3.10+
PY=$(python3 --version 2>&1 | grep -oP '(?<=3\.)\d+' || echo "0")
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

# 3. Installer Airflow avec son constraint file officiel (évite les conflits)
AIRFLOW_VERSION=2.10.4
PYTHON_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

echo "Installation d'Airflow ${AIRFLOW_VERSION} (Python ${PYTHON_VERSION})..."
pip install -q "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# 4. Installer duckdb, dbt-duckdb, pandas
echo "Installation de duckdb, dbt-duckdb, pandas..."
pip install -q -r requirements.txt

# 5. Générer les CSV si absents
if [ ! -f "data/raw/loans.csv" ]; then
  echo "Génération des données synthétiques..."
  python3 scripts/generate_data.py
fi

# 6. Airflow
export AIRFLOW_HOME="$(pwd)/airflow_home"
export ELT_PROJECT_DIR="$(pwd)"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

mkdir -p airflow_home/dags
cp dags/elt_pipeline.py airflow_home/dags/

echo "Initialisation de la base Airflow..."
airflow db migrate

airflow users list 2>/dev/null | grep -q admin || \
  airflow users create \
    --username admin --password admin \
    --firstname Admin --lastname User \
    --role Admin --email admin@example.com

# 7. Valider dbt
echo "Validation dbt..."
cd dbt_project && dbt debug --profiles-dir . 2>&1 | tail -5 && cd ..

echo ""
echo "=== Setup terminé ==="
echo ""
echo "Ouvrir 2 terminaux dans ce dossier :"
echo "  Terminal 1 : source .venv/bin/activate && export AIRFLOW_HOME=\$(pwd)/airflow_home && export ELT_PROJECT_DIR=\$(pwd) && airflow scheduler"
echo "  Terminal 2 : source .venv/bin/activate && export AIRFLOW_HOME=\$(pwd)/airflow_home && airflow webserver -p 8080"
echo "  UI         : http://localhost:8080  (admin / admin)"
