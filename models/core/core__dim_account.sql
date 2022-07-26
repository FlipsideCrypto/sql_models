{{ config(
    materialized = 'incremental',
    unique_key = 'dim_account_id',
    incremental_strategy = 'merge',
    cluster_by = ['created_at::DATE']
) }}

SELECT
    {{ dbt_utils.surrogate_key(
        ['addr :: text']
    ) }} AS dim_account_id,
    algorand_decode_hex_addr(
        addr :: text
    ) AS address,
    deleted AS account_closed,
    CASE
        WHEN rewardsbase > 0 THEN TRUE
        ELSE FALSE
    END non_zero_rewards_base,
    CASE
        WHEN rewards_total > 0 THEN TRUE
        ELSE FALSE
    END non_zero_rewards_total,
    CASE
        WHEN microalgos > 0 THEN TRUE
        ELSE FALSE
    END non_zero_balance,
    COALESCE(
        C.dim_block_id,
        {{ dbt_utils.surrogate_key(
            ['null']
        ) }}
    ) AS dim_block_id__created_at,
    C.block_timestamp AS created_at,
    COALESCE(
        b.dim_block_id,
        {{ dbt_utils.surrogate_key(
            ['null']
        ) }}
    ) AS dim_block_id__closed_at,
    b.block_timestamp AS closed_at,
    COALESCE(
        d.dim_wallet_type_id,
        {{ dbt_utils.surrogate_key(
            ['null']
        ) }}
    ) AS dim_wallet_type_id,
    account_data,
    A._inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    {{ ref('bronze__account') }} A
    LEFT JOIN {{ ref('core__dim_block') }}
    b
    ON A.closed_at = b.block_id
    LEFT JOIN {{ ref('core__dim_block') }} C
    ON A.created_at = C.block_id
    LEFT JOIN {{ ref('core__dim_wallet_type') }}
    d
    ON A.keytype = d.wallet_type

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
UNION ALL
SELECT
    {{ dbt_utils.surrogate_key(
        ['null']
    ) }} AS dim_account_id,
    NULL AS address,
    NULL AS account_closed,
    NULL AS non_zero_rewards_base,
    NULL AS non_zero_rewards_total,
    NULL AS non_zero_balance,
    {{ dbt_utils.surrogate_key(
        ['null']
    ) }} AS dim_block_id__created_at,
    NULL AS created_at,
    {{ dbt_utils.surrogate_key(
        ['null']
    ) }} AS dim_block_id__closed_at,
    NULL AS closed_at,
    {{ dbt_utils.surrogate_key(
        ['null']
    ) }} AS dim_wallet_type_id,
    NULL AS account_data,
    CURRENT_DATE _inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
