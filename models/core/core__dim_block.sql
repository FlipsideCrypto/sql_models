{{ config(
    materialized = 'incremental',
    unique_key = 'dim_block_id',
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE']
) }}

SELECT
    {{ dbt_utils.surrogate_key(
        ['ROUND']
    ) }} AS dim_block_id,
    ROUND AS block_id,
    realtime :: TIMESTAMP AS block_timestamp,
    realtime :: DATE AS block_date,
    rewardslevel AS rewards_level,
    header :gen :: STRING AS network,
    header :gh :: STRING AS genesis_hash,
    header :prev :: STRING AS prev_block_hash,
    header :txn :: STRING AS txn_root,
    header,
    DATEADD(
        ms,
        __HEVO__LOADED_AT,
        '1970-01-01'
    ) AS _INSERTED_TIMESTAMP,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    {{ ref('bronze__block') }}

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
    {{ dbt_utils.surrogate_key(
        ['null']
    ) }} AS dim_block_id,
    NULL AS block_id,
    NULL AS block_timestamp,
    NULL AS block_date,
    NULL AS rewards_level,
    NULL AS network,
    NULL AS genesis_hash,
    NULL AS prev_block_hash,
    NULL AS txn_root,
    NULL AS header,
    CURRENT_DATE _inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
