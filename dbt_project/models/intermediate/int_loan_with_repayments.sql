-- models/intermediate/int_loan_with_repayments.sql
-- Jointure prêts × remboursements.
-- Ce modèle attend que stg_loans ET stg_repayments soient terminés (fan-in).
-- dbt --threads 4 : s'exécute en parallèle avec d'autres modèles intermediate.

{{ config(materialized='table') }}

WITH loans AS (
    SELECT * FROM {{ ref('stg_loans') }}
),

repayments_agg AS (
    SELECT
        loan_id,
        COUNT(*)                                    AS nb_repayments,
        SUM(amount)                                 AS total_repaid,
        MAX(payment_date)                           AS last_payment_date,
        COUNT(*) FILTER (WHERE payment_status = 'LATE') AS nb_late
    FROM {{ ref('stg_repayments') }}
    GROUP BY loan_id
)

SELECT
    l.loan_id,
    l.client_id,
    l.agent_id,
    l.loan_amount,
    l.interest_rate_pct,
    l.disbursement_date,
    l.maturity_date,
    l.duration_months,
    l.status,
    l.currency_code,

    COALESCE(r.nb_repayments,  0)      AS nb_repayments,
    COALESCE(r.total_repaid,   0.0)    AS total_repaid,
    COALESCE(r.nb_late,        0)      AS nb_late_payments,
    r.last_payment_date,

    -- Taux de remboursement (0 à 1, peut dépasser 1 si overpaid)
    ROUND(
        COALESCE(r.total_repaid, 0.0) / NULLIF(l.loan_amount, 0),
        4
    )                                  AS repayment_rate,

    -- Indicateur de bon payeur
    CASE
        WHEN l.status = 'DEFAULTED'              THEN 'bad'
        WHEN COALESCE(r.nb_late, 0) = 0          THEN 'excellent'
        WHEN COALESCE(r.nb_late, 0) <= 2         THEN 'good'
        ELSE                                          'at_risk'
    END                                AS borrower_quality

FROM loans l
LEFT JOIN repayments_agg r USING (loan_id)
