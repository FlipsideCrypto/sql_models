{{ config(
    materialized = 'incremental',
    unique_key = 'dim_block_id',
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE']
) }}

SELECT
    {{ dbt_utils.surrogate_key(
        ['block_id']
    ) }} AS dim_block_id,
    block_id,
    block_timestamp,
    block_date,
    block_hour,
    block_week,
    block_month,
    block_quarter,
    block_year,
    block_DAYOFMONTH,
    block_DAYOFWEEK,
    block_DAYOFYEAR,
    rewards_level,
    network,
    genesis_hash,
    prev_block_hash,
    txn_root,
    header,
    _INSERTED_TIMESTAMP,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    {{ ref('silver__block') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    '-1' AS dim_block_id,
    -1 AS block_id,
    '1900-01-01' :: datetime AS block_timestamp,
    NULL AS block_date,
    NULL AS block_hour,
    NULL AS block_week,
    NULL AS block_month,
    NULL AS block_quarter,
    NULL AS block_year,
    NULL AS block_DAYOFMONTH,
    NULL AS block_DAYOFWEEK,
    NULL AS block_DAYOFYEAR,
    NULL AS rewards_level,
    NULL AS network,
    NULL AS genesis_hash,
    NULL AS prev_block_hash,
    NULL AS txn_root,
    NULL AS header,
    '1900-01-01' :: DATE AS _inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
UNION ALL
SELECT
    '-2' AS dim_block_id,
    -2 AS block_id,
    NULL AS block_timestamp,
    NULL AS block_date,
    NULL AS block_hour,
    NULL AS block_week,
    NULL AS block_month,
    NULL AS block_quarter,
    NULL AS block_year,
    NULL AS block_DAYOFMONTH,
    NULL AS block_DAYOFWEEK,
    NULL AS block_DAYOFYEAR,
    NULL AS rewards_level,
    NULL AS network,
    NULL AS genesis_hash,
    NULL AS prev_block_hash,
    NULL AS txn_root,
    NULL AS header,
    '1900-01-01' :: DATE AS _inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
