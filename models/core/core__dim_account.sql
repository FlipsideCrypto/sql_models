{{ config(
    materialized = 'incremental',
    unique_key = 'dim_account_id',
    incremental_strategy = 'merge',
    cluster_by = ['created_at::DATE']
) }}

SELECT
    {{ dbt_utils.surrogate_key(
        ['address']
    ) }} AS dim_account_id,
    A.address,
    A.account_closed,
    A.non_zero_rewards_base,
    A.non_zero_rewards_total,
    A.non_zero_balance,
    COALESCE(
        C.dim_block_id,
        '-1'
    ) AS dim_block_id__created_at,
    C.block_timestamp AS created_at,
    COALESCE(
        b.dim_block_id,
        '-2'
    ) AS dim_block_id__closed_at,
    b.block_timestamp AS closed_at,
    COALESCE(
        d.dim_wallet_type_id,
        '-2'
    ) AS dim_wallet_type_id,
    A._inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    {{ ref('silver__account') }} A
    LEFT JOIN {{ ref('core__dim_block') }}
    b
    ON A.closed_at = b.block_id
    LEFT JOIN {{ ref('core__dim_block') }} C
    ON A.created_at = C.block_id
    LEFT JOIN {{ ref('core__dim_wallet_type') }}
    d
    ON A.account_data = d.wallet_type

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
    OR address IN (
        SELECT
            address
        FROM
            {{ this }}
        WHERE
            dim_block_id__created_at = '-1'
            OR dim_wallet_type_id = '-1'
    )
{% endif %}
UNION ALL
SELECT
    '-1' AS dim_account_id,
    'unknown' AS address,
    FALSE AS account_closed,
    FALSE AS non_zero_rewards_base,
    FALSE AS non_zero_rewards_total,
    FALSE AS non_zero_balance,
    '-1' AS dim_block_id__created_at,
    NULL AS created_at,
    '-1' AS dim_block_id__closed_at,
    NULL AS closed_at,
    '-1' AS dim_wallet_type_id,
    '1900-01-01' :: DATE _inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
UNION ALL
SELECT
    '-2' AS dim_account_id,
    'not applicable' AS address,
    FALSE AS account_closed,
    FALSE AS non_zero_rewards_base,
    FALSE AS non_zero_rewards_total,
    FALSE AS non_zero_balance,
    '-2' AS dim_block_id__created_at,
    NULL AS created_at,
    '-2' AS dim_block_id__closed_at,
    NULL AS closed_at,
    '-2' AS dim_wallet_type_id,
    '1900-01-01' :: DATE _inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
UNION ALL
SELECT
    {{ dbt_utils.surrogate_key(
        ['address']
    ) }} AS dim_account_id,
    address,
    FALSE AS account_closed,
    FALSE AS non_zero_rewards_base,
    FALSE AS non_zero_rewards_total,
    FALSE AS non_zero_balance,
    '-2' AS dim_block_id__created_at,
    NULL AS created_at,
    '-2' AS dim_block_id__closed_at,
    NULL AS closed_at,
    '-2' AS dim_wallet_type_id,
    '1900-01-01' :: DATE _inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    (
        SELECT
            'XM6FEYVJ2XDU2IBH4OT6VZGW75YM63CM4TC6AV6BD3JZXFJUIICYTVB5EU' AS address
    ) x
