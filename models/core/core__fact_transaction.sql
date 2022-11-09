{{ config(
    materialized = 'incremental',
    unique_key = 'fact_transaction_id',
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE', 'dim_transaction_type_id']
) }}

WITH base AS (

    SELECT
        intra,
        block_id,
        tx_group_id,
        tx_id,
        inner_tx,
        asset_id,
        sender,
        fee,
        tx_type,
        tx_message,
        extra,
        app_id,
        asset_supply,
        asset_parameters,
        asset_address,
        asset_freeze,
        participation_key,
        vrf_public_key,
        vote_first,
        vote_last,
        vote_keydilution,
        receiver,
        asset_sender,
        asset_receiver,
        asset_amount,
        asset_transferred,
        amount,
        _inserted_timestamp
    FROM
        {{ ref('silver__transaction') }}

{% if is_incremental() %}
WHERE
    (
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
                    OR dim_account_id__tx_sender = '-1'
                    OR dim_asset_id = '-1'
                    OR dim_transaction_type_id = '-1'
                )
        )
    )
{% endif %}
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['a.block_id','a.intra']
    ) }} AS fact_transaction_id,
    COALESCE(
        b.dim_block_id,
        '-1'
    ) AS dim_block_id,
    A.block_id,
    b.block_timestamp,
    intra,
    tx_group_id,
    tx_id,
    inner_tx,
    COALESCE(
        da.dim_account_id,
        '-1'
    ) AS dim_account_id__tx_sender,
    A.sender AS tx_sender,
    COALESCE(
        dim_asset_id,
        CASE
            WHEN A.tx_type IN (
                'appl',
                'keyreg'
            ) THEN '-2'
            ELSE '-1'
        END
    ) AS dim_asset_id,
    fee,
    COALESCE(
        dim_transaction_type_id,
        '-1'
    ) AS dim_transaction_type_id,
    tx_message,
    extra,
    COALESCE(
        rec.dim_account_id,
        '-2'
    ) AS dim_account_id__receiver,
    A.receiver,
    COALESCE(
        a_snd.dim_account_id,
        '-2'
    ) AS dim_account_id__asset_sender,
    A.asset_sender,
    COALESCE(
        a_rec.dim_account_id,
        '-2'
    ) AS dim_account_id__asset_receiver,
    A.asset_receiver,
    app_id,
    asset_supply,
    asset_parameters,
    asset_address,
    asset_freeze,
    participation_key,
    vrf_public_key,
    vote_first,
    vote_last,
    vote_keydilution,
    asset_amount,
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
    ON A.sender = da.address
    LEFT JOIN {{ ref('core__dim_account') }}
    rec
    ON A.receiver = rec.address
    LEFT JOIN {{ ref('core__dim_account') }}
    a_snd
    ON A.asset_sender = a_snd.address
    LEFT JOIN {{ ref('core__dim_account') }}
    a_rec
    ON A.asset_receiver = a_rec.address
    LEFT JOIN {{ ref('core__dim_asset') }}
    das
    ON A.asset_id = das.asset_id
    LEFT JOIN {{ ref('core__dim_transaction_type') }}
    dtt
    ON A.tx_type = dtt.tx_type
