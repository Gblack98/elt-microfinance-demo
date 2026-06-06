-- models/staging/stg_clients.sql
{{ config(materialized='view') }}

SELECT
    CAST(client_id        AS VARCHAR) AS client_id,
    TRIM(prenom)                      AS first_name,
    UPPER(TRIM(nom))                  AS last_name,
    CAST(date_naissance   AS DATE)    AS birth_date,
    DATE_DIFF('year',
        CAST(date_naissance AS DATE),
        CURRENT_DATE)                 AS age,
    UPPER(TRIM(genre))                AS gender,
    TRIM(telephone)                   AS phone,
    INITCAP(TRIM(ville))              AS city,
    CAST(date_inscription AS DATE)    AS registration_date
FROM raw.clients
WHERE client_id IS NOT NULL
