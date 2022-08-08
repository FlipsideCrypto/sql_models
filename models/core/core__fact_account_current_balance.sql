{{ config(
    materialized = 'incremental',
    unique_key = 'fact_account_current_balance_id',
    incremental_strategy = 'merge',
    cluster_by = ['created_at::DATE']
) }}

WITH base AS (

    SELECT
        algorand_decode_hex_addr(
            addr :: text
        ) AS address,
        rewardsbase,
        rewards_total,
        microalgos,
        created_at,
        _inserted_timestamp
    FROM
        {{ ref('bronze__account') }} A

{% if is_incremental() %}
WHERE
    A._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['a.address','dim_account_id']
    ) }} AS fact_account_current_balance_id,
    act.dim_account_id,
    A.address,
    A.rewardsbase / pow(
        10,
        6
    ) AS rewards_base,
    A.rewards_total / pow(
        10,
        6
    ) AS rewards_total,
    A.microalgos / pow(
        10,
        6
    ) AS balance,
    C.dim_block_id AS dim_block_id__created_at,
    C.block_timestamp AS created_at,
    A._inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    base A
    JOIN {{ ref('core__dim_account') }}
    act
    ON A.address = act.address
    JOIN {{ ref('core__dim_block') }} C
    ON A.created_at = C.block_id
