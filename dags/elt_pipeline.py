"""
DAG : pipeline ELT de microfinance
Source : fichiers CSV (data/raw/)
Target : DuckDB (warehouse.duckdb)
Transform : dbt-duckdb (staging → intermediate → mart)

Parallélisme :
  - 3 extractions en parallèle (loans, clients, repayments)
  - fan-in sur load_raw
  - 3 modèles staging en parallèle
  - fan-in sur intermediate
  - mart → tests
"""

import os
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
import duckdb

# ─────────────────────────────────────────────────────────
# Chemins
# ─────────────────────────────────────────────────────────
PROJECT_DIR  = Path(os.environ.get("ELT_PROJECT_DIR", Path(__file__).parents[2]))
DATA_DIR     = PROJECT_DIR / "data" / "raw"
DB_PATH      = str(PROJECT_DIR / "warehouse.duckdb")
DBT_DIR      = str(PROJECT_DIR / "dbt_project")
DBT_PROFILES = str(PROJECT_DIR / "dbt_project")   # profiles.yml est dans dbt_project/

# ─────────────────────────────────────────────────────────
# Helpers Python
# ─────────────────────────────────────────────────────────

def extract_and_validate(table: str, **context):
    """
    Lit le CSV, valide les colonnes obligatoires, pousse
    le chemin du fichier dans XCom pour la tâche load.
    """
    path = DATA_DIR / f"{table}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Fichier source introuvable : {path}")

    import csv
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if len(rows) == 0:
        raise ValueError(f"Le fichier {table}.csv est vide")

    print(f"[extract] {table} → {len(rows):,} lignes, colonnes : {list(rows[0].keys())}")
    context["ti"].xcom_push(key=f"row_count_{table}", value=len(rows))
    return str(path)


def load_raw_to_duckdb(**context):
    """
    Charge les 3 CSV dans DuckDB sous le schéma 'raw'.
    Exécutée après le fan-in (les 3 extract tasks doivent être terminées).
    """
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")

    tables = {
        "clients":     DATA_DIR / "clients.csv",
        "loans":       DATA_DIR / "loans.csv",
        "repayments":  DATA_DIR / "repayments.csv",
    }

    for table, path in tables.items():
        con.execute(f"DROP TABLE IF EXISTS raw.{table}")
        con.execute(f"""
            CREATE TABLE raw.{table} AS
            SELECT * FROM read_csv_auto('{path}', header=true)
        """)
        count = con.execute(f"SELECT COUNT(*) FROM raw.{table}").fetchone()[0]
        print(f"[load] raw.{table} → {count:,} lignes chargées")

    con.close()
    print(f"[load] DuckDB prêt : {DB_PATH}")


def run_quality_checks(**context):
    """
    Contrôles qualité post-dbt :
    - Taux de défaut global
    - Prêts sans aucun remboursement (hors DEFAULTED récents)
    - Nulls sur loan_id dans le mart
    """
    con = duckdb.connect(DB_PATH)

    # 1. Taux de défaut
    row = con.execute("""
        SELECT
            COUNT(*) FILTER (WHERE statut = 'DEFAULTED') AS nb_defaults,
            COUNT(*)                                      AS total,
            ROUND(COUNT(*) FILTER (WHERE statut = 'DEFAULTED') * 100.0 / COUNT(*), 2) AS taux
        FROM raw.loans
    """).fetchone()
    print(f"[quality] Taux de défaut : {row[0]}/{row[1]} = {row[2]}%")
    if row[2] > 30:
        raise ValueError(f"Taux de défaut anormalement élevé : {row[2]}%")

    # 2. Nulls dans le mart
    nulls = con.execute("""
        SELECT COUNT(*) FROM main_main.fct_portfolio_summary
        WHERE loan_id IS NULL
    """).fetchone()[0]
    print(f"[quality] Nulls sur loan_id dans fct_portfolio_summary : {nulls}")
    if nulls > 0:
        raise ValueError(f"{nulls} lignes avec loan_id NULL dans le mart")

    con.close()
    print("[quality] Tous les contrôles passés ✓")


# ─────────────────────────────────────────────────────────
# Définition du DAG
# ─────────────────────────────────────────────────────────

default_args = {
    "owner": "data_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="elt_microfinance_pipeline",
    description="Pipeline ELT : CSV → DuckDB → dbt (staging/intermediate/mart)",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_tasks=6,
    tags=["elt", "microfinance", "dbt", "duckdb"],
    default_args=default_args,
    doc_md="""
## ELT Microfinance Pipeline

Pipeline complet de transformation de données de microfinance.

**Sources** : fichiers CSV (clients, prêts, remboursements)
**Target**  : DuckDB (warehouse local)
**Transform**: dbt-duckdb (3 couches)

### Graphe de dépendances
```
extract_loans ─┐
extract_clients─┤─► load_raw ─► [stg_loans    ]─┐
extract_repay──┘              ─► [stg_clients  ]─┤─► int_loan_repayments
                               ─► [stg_repayments]─┘
                                                     │
                                                     ▼
                                              fct_portfolio_summary
                                                     │
                                                     ▼
                                              quality_checks
```
    """,
) as dag:

    # ── 1. EXTRACT (3 tâches parallèles) ──────────────────
    with TaskGroup("extract", tooltip="Extraction et validation des fichiers CSV") as tg_extract:

        extract_loans = PythonOperator(
            task_id="extract_loans",
            python_callable=extract_and_validate,
            op_kwargs={"table": "loans"},
            doc_md="Lit et valide loans.csv (montant > 0, loan_id non nul)",
        )

        extract_clients = PythonOperator(
            task_id="extract_clients",
            python_callable=extract_and_validate,
            op_kwargs={"table": "clients"},
            doc_md="Lit et valide clients.csv (client_id unique)",
        )

        extract_repayments = PythonOperator(
            task_id="extract_repayments",
            python_callable=extract_and_validate,
            op_kwargs={"table": "repayments"},
            doc_md="Lit et valide repayments.csv",
        )

    # ── 2. LOAD (fan-in → attend les 3 extractions) ───────
    load_raw = PythonOperator(
        task_id="load_raw_to_duckdb",
        python_callable=load_raw_to_duckdb,
        doc_md="Charge les 3 CSV dans DuckDB (schéma raw) via read_csv_auto()",
    )

    # Environnement transmis à tous les BashOperator dbt :
    # env= remplace tout l'environnement, donc on doit réinjecter PATH
    # avec le bin du venv pour que `dbt` soit trouvable.
    _venv_bin = str(PROJECT_DIR / ".venv" / "bin")
    _dbt_env = {
        "ELT_PROJECT_DIR": str(PROJECT_DIR),
        "PATH": _venv_bin + ":" + os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
        "HOME": os.environ.get("HOME", ""),
    }

    # ── 3. STAGING dbt (3 modèles parallèles) ─────────────
    with TaskGroup("dbt_staging", tooltip="Nettoyage et typage (vues dbt)") as tg_staging:

        dbt_stg_loans = BashOperator(
            task_id="stg_loans",
            bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir {DBT_PROFILES} --select stg_loans",
            env=_dbt_env,
            doc_md="Caste les types, filtre les nulls, normalise le statut",
        )

        dbt_stg_clients = BashOperator(
            task_id="stg_clients",
            bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir {DBT_PROFILES} --select stg_clients",
            env=_dbt_env,
            doc_md="Normalise le genre, calcule l'âge, formate le téléphone",
        )

        dbt_stg_repayments = BashOperator(
            task_id="stg_repayments",
            bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir {DBT_PROFILES} --select stg_repayments",
            env=_dbt_env,
            doc_md="Valide les montants, typage date",
        )

    # ── 4. INTERMEDIATE (fan-in → attend les 3 staging) ───
    dbt_intermediate = BashOperator(
        task_id="dbt_int_loan_repayments",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES} --select int_loan_with_repayments"
        ),
        env=_dbt_env,
        doc_md="Jointure prêts × remboursements, calcul taux de remboursement",
    )

    # ── 5. MART ───────────────────────────────────────────
    dbt_mart = BashOperator(
        task_id="dbt_fct_portfolio_summary",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES} --select fct_portfolio_summary"
        ),
        env=_dbt_env,
        doc_md="Agrégation mensuelle par devise : volume, taux de défaut, taux de remboursement",
    )

    # ── 6. TESTS dbt ──────────────────────────────────────
    dbt_tests = BashOperator(
        task_id="dbt_tests",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES}"
        ),
        env=_dbt_env,
        doc_md="Tests de qualité définis dans schema.yml (not_null, unique, accepted_values)",
    )

    # ── 7. QUALITY CHECKS custom ──────────────────────────
    quality = PythonOperator(
        task_id="quality_checks",
        python_callable=run_quality_checks,
        doc_md="Vérifie le taux de défaut global et les nulls dans le mart",
    )

    # ── DÉPENDANCES ───────────────────────────────────────
    tg_extract >> load_raw >> tg_staging >> dbt_intermediate >> dbt_mart >> dbt_tests >> quality
