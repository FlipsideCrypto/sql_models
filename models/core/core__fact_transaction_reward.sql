{{ config(
    materialized = 'incremental',
    unique_key = 'fact_transaction_close_id',
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

    SELECT
        block_id,
        intra,
        tx_group_id,
        tx_id,
        inner_tx,
        account,
        asset_id,
        amount,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__transaction_close') }}

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
    OR tx_id IN (
        SELECT
            tx_id
        FROM
            {{ this }}
        WHERE
            (
                dim_block_id = '-1'
                OR dim_account_id = '-1'
                OR dim_asset_id = '-1'
            )
    )
{% endif %}
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['a.block_id','a.intra','a.account']
    ) }} AS fact_transaction_close_id,
    COALESCE(
        b.dim_block_id,
        '-1'
    ) AS dim_block_id,
    b.block_timestamp,
    intra,
    tx_group_id,
    tx_id,
    inner_tx,
    COALESCE(
        da.dim_account_id,
        '-1'
    ) AS dim_account_id,
    A.account AS address,
    COALESCE(
        dim_asset_id,
        '-1'
    ) AS dim_asset_id,
    amount,
    A._inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    base A
    JOIN {{ ref('core__dim_block') }}
    b
    ON A.block_id = b.block_id
    LEFT JOIN {{ ref('core__dim_account') }}
    da
    ON A.account = da.address
    LEFT JOIN {{ ref('core__dim_asset') }}
    das
    ON A.asset_id = das.asset_id
