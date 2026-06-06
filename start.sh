#!/usr/bin/env bash
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

VENV="$PROJECT_DIR/.venv"
AIRFLOW_BIN="$VENV/bin/airflow"
STREAMLIT_BIN="$VENV/bin/streamlit"
PYTHON_BIN="$VENV/bin/python3"
AIRFLOW_HOME="$PROJECT_DIR/airflow_home"
LOG_DIR="$PROJECT_DIR/logs/run"
mkdir -p "$LOG_DIR"

open_url() {
    if [[ "$OSTYPE" == "darwin"* ]]; then
        open "$1"
    else
        xdg-open "$1" 2>/dev/null || true
    fi
}

PIDS=()
cleanup() {
    echo ""
    echo "Arrêt des services..."
    for pid in "${PIDS[@]:-}"; do
        kill "$pid" 2>/dev/null || true
    done
    exit 0
}
trap cleanup INT TERM

# ── 1. Setup si pas encore fait ──────────────────────────────────────────────
if [ ! -f "$AIRFLOW_BIN" ]; then
    echo "Premier lancement — installation en cours (3-5 min)..."
    bash "$PROJECT_DIR/init.sh"
fi

export AIRFLOW_HOME
export ELT_PROJECT_DIR="$PROJECT_DIR"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export PATH="$VENV/bin:$PATH"

# ── 2. Copier le DAG ─────────────────────────────────────────────────────────
mkdir -p "$AIRFLOW_HOME/dags"
cp "$PROJECT_DIR/dags/elt_pipeline.py" "$AIRFLOW_HOME/dags/"

# ── 3. Scheduler ─────────────────────────────────────────────────────────────
echo "Démarrage du scheduler Airflow..."
"$AIRFLOW_BIN" scheduler > "$LOG_DIR/scheduler.log" 2>&1 &
PIDS+=($!)
sleep 3

# Vérifier que le scheduler est bien lancé
if ! kill -0 "${PIDS[-1]}" 2>/dev/null; then
    echo "Erreur scheduler — dernières lignes du log :"
    tail -10 "$LOG_DIR/scheduler.log"
    exit 1
fi

# ── 4. Webserver ──────────────────────────────────────────────────────────────
echo "Démarrage du webserver Airflow..."
"$AIRFLOW_BIN" webserver -p 8080 > "$LOG_DIR/webserver.log" 2>&1 &
PIDS+=($!)

# ── 5. Attendre le webserver (max 3 min) ─────────────────────────────────────
echo -n "Attente du webserver"
WAIT=0
READY=false
while [ "$WAIT" -lt 180 ]; do
    if curl -s "http://localhost:8080/health" 2>/dev/null | grep -q "healthy"; then
        READY=true
        break
    fi
    if curl -s "http://localhost:8080/" 2>/dev/null | grep -qi "airflow"; then
        READY=true
        break
    fi
    # Vérifier que le webserver n'a pas crashé
    if ! kill -0 "${PIDS[-1]}" 2>/dev/null; then
        echo ""
        echo "Le webserver a crashé. Log :"
        tail -20 "$LOG_DIR/webserver.log"
        exit 1
    fi
    printf "."
    sleep 3
    WAIT=$((WAIT + 3))
done

if [ "$READY" = true ]; then
    echo " ✓"
else
    echo ""
    echo "Webserver lent — il est peut-être prêt, ouvre http://localhost:8080"
fi

# ── 6. Ouvrir Airflow ────────────────────────────────────────────────────────
open_url "http://localhost:8080"
echo "Airflow → http://localhost:8080  (admin / admin)"

# ── 7. Déclencher le DAG ─────────────────────────────────────────────────────
echo "Déclenchement du pipeline ELT..."
"$AIRFLOW_BIN" dags unpause elt_microfinance_pipeline > /dev/null 2>&1 || true
"$AIRFLOW_BIN" dags trigger elt_microfinance_pipeline > /dev/null 2>&1 || true
echo "Pipeline lancé ✓"

# ── 8. Attendre la fin du DAG ────────────────────────────────────────────────
echo -n "Pipeline en cours"
for i in $(seq 1 72); do
    STATE=$("$PYTHON_BIN" - <<'PYEOF'
import subprocess, json, sys, os
env = os.environ.copy()
r = subprocess.run(
    ["airflow", "dags", "list-runs", "-d", "elt_microfinance_pipeline", "--output", "json"],
    capture_output=True, text=True, env=env
)
try:
    runs = json.loads(r.stdout)
    print(runs[0]["state"] if runs else "queued")
except Exception:
    print("queued")
PYEOF
)
    if [ "$STATE" = "success" ]; then
        echo " ✓ Pipeline terminé"
        break
    elif [ "$STATE" = "failed" ]; then
        echo ""
        echo "Pipeline échoué — voir http://localhost:8080"
        break
    fi
    printf "."
    sleep 5
done

# ── 9. Streamlit ─────────────────────────────────────────────────────────────
echo "Démarrage du dashboard Streamlit..."
"$STREAMLIT_BIN" run "$PROJECT_DIR/dashboard.py" \
    --server.port 8501 --server.headless true \
    > "$LOG_DIR/streamlit.log" 2>&1 &
PIDS+=($!)
sleep 4
open_url "http://localhost:8501"

echo ""
echo "╔══════════════════════════════════════════╗"
echo "║      ELT Microfinance Demo — Prêt        ║"
echo "╠══════════════════════════════════════════╣"
echo "║  Airflow   →  http://localhost:8080      ║"
echo "║  login : admin / admin                   ║"
echo "║  Dashboard →  http://localhost:8501      ║"
echo "╠══════════════════════════════════════════╣"
echo "║  Ctrl+C pour tout arrêter                ║"
echo "╚══════════════════════════════════════════╝"

wait
