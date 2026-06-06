#!/usr/bin/env bash
# Lance tout en une commande :
# setup → scheduler → webserver → DAG → streamlit → ouvre les URLs

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

VENV="$PROJECT_DIR/.venv"
AIRFLOW_HOME="$PROJECT_DIR/airflow_home"
LOG_DIR="$PROJECT_DIR/logs/run"
mkdir -p "$LOG_DIR"

# ── Ouvre une URL dans le navigateur (Mac + Linux) ───────────────────────────
open_url() {
    if [[ "$OSTYPE" == "darwin"* ]]; then
        open "$1"
    else
        xdg-open "$1" 2>/dev/null || true
    fi
}

# ── Cleanup sur Ctrl+C ───────────────────────────────────────────────────────
PIDS=()
cleanup() {
    echo ""
    echo "Arrêt des services..."
    for pid in "${PIDS[@]:-}"; do
        kill "$pid" 2>/dev/null || true
    done
    echo "Terminé."
    exit 0
}
trap cleanup INT TERM

# ── 1. Setup si pas encore fait ──────────────────────────────────────────────
if [ ! -d "$VENV" ]; then
    echo "Premier lancement — installation en cours (2-5 min)..."
    bash "$PROJECT_DIR/init.sh"
fi

source "$VENV/bin/activate"
export AIRFLOW_HOME
export ELT_PROJECT_DIR="$PROJECT_DIR"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

# ── 2. Copier le DAG (au cas où il a changé) ─────────────────────────────────
mkdir -p "$AIRFLOW_HOME/dags"
cp "$PROJECT_DIR/dags/elt_pipeline.py" "$AIRFLOW_HOME/dags/"

# ── 3. Démarrer le scheduler ─────────────────────────────────────────────────
echo "Démarrage du scheduler Airflow..."
airflow scheduler > "$LOG_DIR/scheduler.log" 2>&1 &
PIDS+=($!)

# ── 4. Démarrer le webserver ─────────────────────────────────────────────────
echo "Démarrage du webserver Airflow..."
airflow webserver -p 8080 > "$LOG_DIR/webserver.log" 2>&1 &
PIDS+=($!)

# ── 5. Attendre que le webserver soit prêt ───────────────────────────────────
echo -n "Attente du webserver"
until curl -s "http://localhost:8080/api/v1/health" 2>/dev/null | grep -q "healthy"; do
    printf "."
    sleep 2
done
echo " ✓"

# ── 6. Ouvrir Airflow dans le navigateur ─────────────────────────────────────
open_url "http://localhost:8080"
echo "Airflow → http://localhost:8080  (admin / admin)"

# ── 7. Déclencher le DAG ─────────────────────────────────────────────────────
echo "Déclenchement du pipeline ELT..."
airflow dags unpause elt_microfinance_pipeline > /dev/null 2>&1 || true
airflow dags trigger elt_microfinance_pipeline > /dev/null 2>&1
echo "Pipeline lancé ✓"

# ── 8. Attendre la fin du DAG ────────────────────────────────────────────────
echo -n "Pipeline en cours"
for i in $(seq 1 72); do   # timeout ~6 min
    STATE=$(python3 - <<'PYEOF'
import subprocess, json, sys
r = subprocess.run(
    ["airflow", "dags", "list-runs", "-d", "elt_microfinance_pipeline", "--output", "json"],
    capture_output=True, text=True
)
try:
    runs = json.loads(r.stdout)
    print(runs[0]["state"] if runs else "queued")
except Exception:
    print("queued")
PYEOF
)
    if [ "$STATE" = "success" ]; then
        echo " ✓ Pipeline terminé avec succès"
        break
    elif [ "$STATE" = "failed" ]; then
        echo ""
        echo "Pipeline échoué — consulte http://localhost:8080 pour voir les logs"
        echo "Le dashboard Streamlit va quand même démarrer si des données existent."
        break
    fi
    printf "."
    sleep 5
done

# ── 9. Démarrer Streamlit ────────────────────────────────────────────────────
echo "Démarrage du dashboard Streamlit..."
streamlit run "$PROJECT_DIR/dashboard.py" \
    --server.port 8501 \
    --server.headless true \
    > "$LOG_DIR/streamlit.log" 2>&1 &
PIDS+=($!)

sleep 3
open_url "http://localhost:8501"

# ── Résumé ───────────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║         ELT Microfinance Demo — Prêt         ║"
echo "╠══════════════════════════════════════════════╣"
echo "║  Airflow   →  http://localhost:8080           ║"
echo "║              login : admin / admin            ║"
echo "║  Dashboard →  http://localhost:8501           ║"
echo "╠══════════════════════════════════════════════╣"
echo "║  Ctrl+C pour tout arrêter                    ║"
echo "╚══════════════════════════════════════════════╝"

wait
