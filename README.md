# ELT Microfinance Demo

Pipeline ELT complet : **CSV → DuckDB → dbt → Dashboard Streamlit**, orchestré par Apache Airflow.

Construit pour illustrer le **parallélisme** dans un pipeline de données réel.

---

## Démarrage en une seule commande

### Mac

```bash
git clone https://github.com/Gblack98/elt-microfinance-demo.git
cd elt-microfinance-demo
bash start.sh
```

### Windows (Git Bash)

> Prérequis : [Git for Windows](https://git-scm.com/download/win) + [Python 3.11+](https://www.python.org/downloads/) (cocher "Add to PATH")

```bash
git clone https://github.com/Gblack98/elt-microfinance-demo.git
cd elt-microfinance-demo
bash start.sh
```

Le script fait tout automatiquement :
1. Installe les dépendances (première fois seulement)
2. Démarre Airflow scheduler + webserver
3. Déclenche le pipeline ELT
4. Démarre le dashboard Streamlit
5. Ouvre les URLs dans le navigateur

---

## URLs

| Service   | URL                    | Login         |
|-----------|------------------------|---------------|
| Airflow   | http://localhost:8080  | admin / admin |
| Dashboard | http://localhost:8501  | —             |

---

## Architecture du pipeline

```
CSV (clients, loans, repayments)
        │
        ▼
┌─────────────────────────────────┐
│  EXTRACT  (3 tâches parallèles) │
│  extract_loans                  │
│  extract_clients   ──────────►  │
│  extract_repayments             │
└─────────────┬───────────────────┘
              ▼
      load_raw_to_duckdb
              │
              ▼
┌─────────────────────────────────┐
│  STAGING dbt  (3 en parallèle)  │
│  stg_loans                      │
│  stg_clients       ──────────►  │
│  stg_repayments                 │
└─────────────┬───────────────────┘
              ▼
    int_loan_with_repayments
              │
              ▼
      fct_portfolio_summary
              │
              ▼
       dbt_tests + quality_checks
```

## Stack

| Outil        | Rôle                        |
|--------------|-----------------------------|
| Airflow 2.10 | Orchestration & parallélisme |
| DuckDB       | Data warehouse local        |
| dbt-duckdb   | Transformations SQL         |
| Streamlit    | Dashboard de visualisation  |
| Plotly       | Graphiques                  |
