import os
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

# ── Config ────────────────────────────────────────────────────────────────────
DB_PATH = Path(os.environ.get("ELT_PROJECT_DIR", Path(__file__).parent)) / "warehouse.duckdb"

st.set_page_config(
    page_title="Microfinance Portfolio",
    page_icon="📊",
    layout="wide",
)

# ── Chargement des données ────────────────────────────────────────────────────
@st.cache_data
def load_data():
    con = duckdb.connect(str(DB_PATH), read_only=True)
    df = con.execute("SELECT * FROM main_main.fct_portfolio_summary").df()
    con.close()
    df["disbursement_month"] = pd.to_datetime(df["disbursement_month"])
    return df

try:
    df = load_data()
except Exception as e:
    st.error(f"Impossible de lire warehouse.duckdb : {e}")
    st.info("Lance d'abord le DAG Airflow pour générer les données.")
    st.stop()

# ── En-tête ───────────────────────────────────────────────────────────────────
st.title("📊 Microfinance Portfolio — ELT Demo")
st.caption("Source : pipeline Airflow + dbt + DuckDB · table `fct_portfolio_summary`")
st.divider()

# ── KPIs ──────────────────────────────────────────────────────────────────────
total_loans    = len(df)
total_volume   = df["loan_amount"].sum()
default_rate   = df["is_default"].mean() * 100
repayment_rate = df["repayment_rate"].mean() * 100

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total prêts",        f"{total_loans:,}")
k2.metric("Volume total",       f"{total_volume:,.0f} XOF")
k3.metric("Taux de défaut",     f"{default_rate:.1f}%")
k4.metric("Remboursement moyen",f"{repayment_rate:.1f}%")

st.divider()

# ── Graphiques ────────────────────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Répartition des statuts")
    status_counts = df["status"].value_counts().reset_index()
    status_counts.columns = ["Statut", "Nombre"]
    fig1 = px.pie(
        status_counts,
        names="Statut",
        values="Nombre",
        color_discrete_sequence=px.colors.qualitative.Set2,
        hole=0.4,
    )
    fig1.update_layout(margin=dict(t=10, b=10), showlegend=True)
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    st.subheader("Taux de défaut par devise")
    by_currency = (
        df.groupby("currency_code")["is_default"]
        .mean()
        .mul(100)
        .round(1)
        .reset_index()
    )
    by_currency.columns = ["Devise", "Taux de défaut (%)"]
    fig2 = px.bar(
        by_currency,
        x="Devise",
        y="Taux de défaut (%)",
        color="Devise",
        color_discrete_sequence=px.colors.qualitative.Set2,
        text_auto=True,
    )
    fig2.update_layout(margin=dict(t=10, b=10), showlegend=False)
    st.plotly_chart(fig2, use_container_width=True)

st.subheader("Volume prêté par mois")
by_month = (
    df.groupby("disbursement_month")["loan_amount"]
    .sum()
    .reset_index()
)
by_month.columns = ["Mois", "Volume (XOF)"]
fig3 = px.bar(
    by_month,
    x="Mois",
    y="Volume (XOF)",
    color_discrete_sequence=["#4C9BE8"],
)
fig3.update_layout(margin=dict(t=10, b=10))
st.plotly_chart(fig3, use_container_width=True)

st.divider()

# ── Table brute ───────────────────────────────────────────────────────────────
st.subheader("Table `fct_portfolio_summary` (20 premières lignes)")
cols = ["loan_id", "client_id", "city", "currency_code", "status",
        "loan_amount", "total_repaid", "repayment_rate", "nb_late_payments", "disbursement_date"]
st.dataframe(df[cols].head(20), use_container_width=True)
