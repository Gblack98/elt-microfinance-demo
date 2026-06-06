# ELT Microfinance – Pipeline de démonstration

Pipeline ELT complet illustrant le parallélisme dans Apache Airflow avec dbt-DuckDB.

## Stack

| Outil | Rôle |
|---|---|
| **CSV** (data/raw/) | Source (clients, prêts, remboursements synthétiques) |
| **Apache Airflow** | Orchestration + parallélisme de tâches |
| **DuckDB** | Data Warehouse local (fichier `warehouse.duckdb`) |
| **dbt-duckdb** | Transformations SQL (staging → intermediate → mart) |

## Structure du projet

```
elt_demo/
├── data/raw/               # CSVs sources (générés)
│   ├── clients.csv         # 300 clients
│   ├── loans.csv           # 600 prêts
│   └── repayments.csv      # ~6 200 remboursements
├── dags/
│   └── elt_pipeline.py     # DAG Airflow principal
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/        # stg_loans, stg_clients, stg_repayments (vues)
│       ├── intermediate/   # int_loan_with_repayments (table)
│       └── marts/          # fct_portfolio_summary (table)
├── scripts/
│   └── generate_data.py    # Génère les CSV synthétiques
└── requirements.txt
```

## Installation

```bash
# 1. Créer et activer un environnement virtuel
python3 -m venv .venv
source .venv/bin/activate

# 2. Installer les dépendances
pip install -r requirements.txt

# 3. Générer les données CSV
python3 scripts/generate_data.py

# 4. Initialiser Airflow (stocké dans ./airflow_home)
export AIRFLOW_HOME=$(pwd)/airflow_home
export ELT_PROJECT_DIR=$(pwd)
airflow db migrate

# 5. Créer un utilisateur admin
airflow users create \
    --username admin --password admin \
    --firstname Admin --lastname User \
    --role Admin --email admin@example.com

# 6. Copier le DAG dans airflow_home
mkdir -p airflow_home/dags
cp dags/elt_pipeline.py airflow_home/dags/

# 7. Lancer le scheduler + webserver (deux terminaux)
# Terminal 1 :
airflow scheduler

# Terminal 2 :
airflow webserver --port 8080
```

## Lancer la pipeline

```bash
# Via l'UI : http://localhost:8080
# Login : admin / admin
# Activer le DAG "elt_microfinance_pipeline" et déclencher manuellement

# Ou en ligne de commande :
airflow dags trigger elt_microfinance_pipeline
```

## Parallélisme du DAG

```
[extract_loans]     ─┐
[extract_clients]   ─┤─► [load_raw_to_duckdb]
[extract_repayments]─┘
                            │
                            ▼
                    ┌──────────────────────┐
                    │ [stg_loans]          │ (parallèles)
                    │ [stg_clients]        │
                    │ [stg_repayments]     │
                    └──────────────────────┘
                            │
                            ▼
                    [int_loan_with_repayments]
                            │
                            ▼
                    [fct_portfolio_summary]
                            │
                            ▼
                    [dbt_tests] → [quality_checks]
```

## Explorer le résultat

```python
import duckdb
con = duckdb.connect("warehouse.duckdb")

# Vue d'ensemble du portefeuille
con.sql("""
    SELECT
        currency_code,
        status,
        COUNT(*) AS nb_loans,
        ROUND(SUM(loan_amount)/1e6, 2) AS volume_M,
        ROUND(AVG(repayment_rate)*100, 1) AS avg_repayment_pct
    FROM fct_portfolio_summary
    GROUP BY 1, 2
    ORDER BY 1, 3 DESC
""").show()
```
