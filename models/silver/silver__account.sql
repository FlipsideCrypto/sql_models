{{ config(
    materialized = 'incremental',
    unique_key = 'address',
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

SELECT
    algorand_decode_hex_addr(
        addr :: text
    ) AS address,
    addr :: text AS address_raw,
    deleted AS account_closed,
    rewardsbase,
    CASE
        WHEN rewardsbase > 0 THEN TRUE
        ELSE FALSE
    END non_zero_rewards_base,
    rewards_total,
    CASE
        WHEN rewards_total > 0 THEN TRUE
        ELSE FALSE
    END non_zero_rewards_total,
    microalgos,
    CASE
        WHEN microalgos > 0 THEN TRUE
        ELSE FALSE
    END non_zero_balance,
    A.created_at,
    A.closed_at,
    A.keytype account_data,
    A._inserted_timestamp
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
