{{ config(
    materialized = 'incremental',
    unique_key = 'application_id',
    incremental_strategy = 'merge'
) }}

SELECT
    application_id,
    NAME,
    SYSDATE() AS _inserted_timestamp
FROM
    {{ ref('bronze__application_names') }}
