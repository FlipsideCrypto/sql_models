{{ config(
    materialized = 'incremental',
    unique_key = 'dim_wallet_type_id',
    incremental_strategy = 'merge'
) }}

SELECT
    {{ dbt_utils.surrogate_key(
        ['account_data']
    ) }} AS dim_wallet_type_id,
    account_data AS wallet_type,
    MAX(_inserted_timestamp) _inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    {{ ref('silver__account') }}
WHERE
    account_data IS NOT NULL

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
    '-1' AS dim_wallet_type_id,
    'unknown' AS wallet_type,
    '1900-01-01' :: DATE _inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
UNION ALL
SELECT
    '-2' AS dim_wallet_type_id,
    'not applicable' AS wallet_type,
    '1900-01-01' :: DATE _inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
