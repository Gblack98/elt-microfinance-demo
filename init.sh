#!/usr/bin/env bash
set -euo pipefail

echo "=== ELT Microfinance Demo — Setup ==="

# Neutraliser tout AIRFLOW_HOME défini ailleurs
unset AIRFLOW_HOME

# ── 1. Trouver Python 3.12 (Airflow ne supporte pas 3.13+) ───────────────────
find_python() {
    for cmd in python3.12 python3.11 python3.10 python3; do
        if command -v "$cmd" &>/dev/null; then
            MINOR=$("$cmd" -c "import sys; print(sys.version_info.minor)" 2>/dev/null || echo "0")
            MAJOR=$("$cmd" -c "import sys; print(sys.version_info.major)" 2>/dev/null || echo "0")
            if [ "$MAJOR" -eq 3 ] && [ "$MINOR" -ge 10 ] && [ "$MINOR" -le 12 ]; then
                echo "$cmd"
                return 0
            fi
        fi
    done
    return 1
}

PYTHON=$(find_python || true)

if [ -z "$PYTHON" ]; then
    echo ""
    echo "Python 3.10-3.12 introuvable (Airflow ne supporte pas Python 3.13+)."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        echo "Installation de Python 3.12 via Homebrew..."
        if ! command -v brew &>/dev/null; then
            echo "Homebrew requis. Installe-le : https://brew.sh"
            exit 1
        fi
        brew install python@3.12 -q
        PYTHON=$(brew --prefix python@3.12)/bin/python3.12
    else
        echo "Installe Python 3.12 : sudo apt install python3.12 python3.12-venv"
        exit 1
    fi
fi

PY_VERSION=$("$PYTHON" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo "Python utilisé : $PYTHON ($PY_VERSION)"

# ── 2. Venv ───────────────────────────────────────────────────────────────────
if [ ! -d ".venv" ]; then
    echo "Création du venv..."
    "$PYTHON" -m venv .venv
fi
source .venv/bin/activate
pip install -q --upgrade pip

# ── 3. Airflow avec constraint file ──────────────────────────────────────────
AIRFLOW_VERSION=2.10.4
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY_VERSION}.txt"

echo "Installation d'Airflow ${AIRFLOW_VERSION}..."
pip install -q "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# ── 4. duckdb, dbt, streamlit, plotly ────────────────────────────────────────
echo "Installation des dépendances data..."
pip install -q -r requirements.txt

# ── 5. Générer les CSV si absents ─────────────────────────────────────────────
if [ ! -f "data/raw/loans.csv" ]; then
    echo "Génération des données synthétiques..."
    python3 scripts/generate_data.py
fi

# ── 6. Initialiser Airflow ────────────────────────────────────────────────────
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

# ── 7. Valider dbt ────────────────────────────────────────────────────────────
echo "Validation dbt..."
cd dbt_project && dbt debug --profiles-dir . 2>&1 | tail -3 && cd ..

echo ""
echo "=== Setup terminé — lance : bash start.sh ==="
