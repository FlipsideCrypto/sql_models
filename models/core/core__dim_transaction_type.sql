{{ config(
    materialized = 'incremental',
    unique_key = 'dim_transaction_type_id',
    incremental_strategy = 'merge'
) }}

WITH base AS (

    SELECT
        {{ dbt_utils.surrogate_key(
            ['tx_type']
        ) }} AS dim_transaction_type_id,
        tx_type,
        MAX(_inserted_timestamp) _inserted_timestamp
    FROM
        {{ ref('silver__transaction') }}
    WHERE
        tx_type IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    dim_transaction_type_id,
    tx_type
)
SELECT
    dim_transaction_type_id,
    tx_type,
    CASE
        tx_type
        WHEN 'pay' THEN 'payment'
        WHEN 'keyreg' THEN 'key registration'
        WHEN 'acfg' THEN 'asset configuration'
        WHEN 'axfer' THEN 'asset transfer'
        WHEN 'afrz' THEN 'asset freeze'
        WHEN 'appl' THEN 'application call'
    END tx_type_name,
    _inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    base
UNION ALL
SELECT
    {{ dbt_utils.surrogate_key(
        ['null']
    ) }}
    dim_transaction_type_id,
    NULL AS tx_type,
    NULL AS tx_type_name,
    CURRENT_DATE AS _inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
