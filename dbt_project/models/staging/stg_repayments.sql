-- models/staging/stg_repayments.sql
{{ config(materialized='view') }}

SELECT
    CAST(repayment_id     AS VARCHAR)        AS repayment_id,
    CAST(loan_id          AS VARCHAR)        AS loan_id,
    CAST(montant          AS DECIMAL(15,2))  AS amount,
    CAST(date_paiement    AS DATE)           AS payment_date,
    UPPER(TRIM(statut_paiement))             AS payment_status
FROM raw.repayments
WHERE repayment_id IS NOT NULL
  AND loan_id      IS NOT NULL
  AND montant      > 0
