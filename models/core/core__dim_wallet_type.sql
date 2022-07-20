{{ config(
    materialized = 'incremental',
    unique_key = 'dim_wallet_type_id',
    incremental_strategy = 'merge'
) }}

SELECT
    {{ dbt_utils.surrogate_key(
        ['keytype']
    ) }} AS dim_wallet_type_id,
    keytype AS wallet_type,
    MAX(_inserted_timestamp) _inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    {{ ref('bronze__account') }}
WHERE
    keytype IS NOT NULL

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
    dim_wallet_type_id,
    wallet_type
UNION ALL
SELECT
    {{ dbt_utils.surrogate_key(
        ['null']
    ) }} AS dim_wallet_type_id,
    NULL AS wallet_type,
    CURRENT_DATE _inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
