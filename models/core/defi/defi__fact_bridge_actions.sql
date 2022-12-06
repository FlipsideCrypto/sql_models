{{ config(
    materialized = 'incremental',
    unique_key = 'fact_bridge_action_id',
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

    SELECT
        bridge,
        block_id,
        intra,
        tx_id,
        asset_id,
        amount,
        bridger_address,
        bridge_address,
        direction,
        _inserted_timestamp
    FROM
        {{ ref('silver__bridge') }}

{% if is_incremental() %}
WHERE
    _INSERTED_TIMESTAMP >= (
        SELECT
            MAX(
                _INSERTED_TIMESTAMP
            )
        FROM
            {{ this }}
    ) - INTERVAL '4 HOURS'
    OR tx_id IN (
        SELECT
            tx_id
        FROM
            {{ this }}
        WHERE
            dim_block_id = '-1'
            OR dim_asset_id = '-1'
            OR dim_account_id__bridger = '-1'
            OR dim_account_id__bridge = '-1'
    )
{% endif %}
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['a.block_id','a.intra','a.bridge']
    ) }} AS fact_bridge_action_id,
    bridge,
    COALESCE(
        f.block_timestamp,
        '1900-01-01' :: DATE
    ) block_timestamp,
    COALESCE(
        f.dim_block_id,
        '-1'
    ) AS dim_block_id,
    A.intra,
    A.tx_id,
    COALESCE(
        b.dim_account_id,
        '-1'
    ) AS dim_account_id__bridger,
    A.bridger_address,
    COALESCE(
        C.dim_asset_id,
        '-1'
    ) AS dim_asset_id,
    A.asset_id,
    CASE
        WHEN A.asset_id = 0 THEN A.amount / pow(
            10,
            6
        )
        WHEN C.decimals > 0 THEN A.amount / pow(
            10,
            C.decimals
        )
        ELSE A.amount
    END :: FLOAT AS amount,
    A.bridge_address,
    COALESCE(
        d.dim_account_id,
        '-1'
    ) AS dim_account_id__bridge,
    A.direction,
    A._inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    base A
    LEFT JOIN {{ ref('core__dim_account') }}
    b
    ON A.bridger_address = b.address
    LEFT JOIN {{ ref('core__dim_asset') }} C
    ON A.asset_id = C.asset_id
    LEFT JOIN {{ ref('core__dim_account') }}
    d
    ON A.bridge_address = d.address
    LEFT JOIN {{ ref('core__dim_block') }}
    f
    ON A.block_id = f.block_id
