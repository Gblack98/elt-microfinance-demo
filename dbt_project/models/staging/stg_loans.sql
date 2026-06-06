-- models/staging/stg_loans.sql
-- Source : raw.loans (chargé depuis loans.csv)
-- Nettoyage, typage, normalisation du statut

{{ config(materialized='view') }}

SELECT
    CAST(loan_id            AS VARCHAR)   AS loan_id,
    CAST(client_id          AS VARCHAR)   AS client_id,
    CAST(agent_id           AS VARCHAR)   AS agent_id,
    CAST(montant_principal  AS DECIMAL(15,2)) AS loan_amount,
    CAST(taux_interet       AS DECIMAL(5,2))  AS interest_rate_pct,
    CAST(date_decaissement  AS DATE)      AS disbursement_date,
    CAST(date_echeance      AS DATE)      AS maturity_date,
    CAST(duree_mois         AS INTEGER)   AS duration_months,
    UPPER(TRIM(statut))                   AS status,
    UPPER(TRIM(devise))                   AS currency_code
FROM raw.loans
WHERE loan_id          IS NOT NULL
  AND client_id        IS NOT NULL
  AND montant_principal > 0
  AND date_decaissement IS NOT NULL
