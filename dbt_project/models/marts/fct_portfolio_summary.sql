-- models/marts/fct_portfolio_summary.sql
-- Agrégation mensuelle du portefeuille de prêts.
-- Enrichi avec les infos client (ville, genre) pour le reporting.

{{ config(materialized='table') }}

WITH loans AS (
    SELECT * FROM {{ ref('int_loan_with_repayments') }}
),

clients AS (
    SELECT client_id, city, gender, age
    FROM {{ ref('stg_clients') }}
)

SELECT
    -- Clés de dimension
    l.loan_id,
    l.client_id,
    l.agent_id,
    c.city,
    c.gender,
    c.age,
    l.currency_code,
    l.status,
    l.borrower_quality,

    -- Dates
    l.disbursement_date,
    l.maturity_date,
    DATE_TRUNC('month', l.disbursement_date)  AS disbursement_month,

    -- Métriques financières
    l.loan_amount,
    l.interest_rate_pct,
    l.duration_months,
    l.total_repaid,
    l.repayment_rate,
    l.nb_repayments,
    l.nb_late_payments,
    l.last_payment_date,

    -- Métriques dérivées
    ROUND(l.loan_amount * l.interest_rate_pct / 100, 2) AS interest_expected,
    l.loan_amount - l.total_repaid                      AS outstanding_amount,

    -- Flags
    (l.status = 'DEFAULTED')    AS is_default,
    (l.repayment_rate >= 1.0)   AS is_fully_repaid,
    (l.nb_late_payments > 0)    AS has_late_payments

FROM loans l
LEFT JOIN clients c USING (client_id)
