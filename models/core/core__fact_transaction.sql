{{ config(
    materialized = 'incremental',
    unique_key = 'fact_transaction_id',
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE']
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
        COALESCE(
            tx_message :txn :apid,
            tx_message :apid,
            tx_message :"dt" :"gd" :"aWQ=" :"ui"
        ) app_id,
        tx_message :txn :apar :t AS asset_supply,
        tx_message :txn :apar AS asset_parameters,
        tx_message :txn :fadd :: text AS asset_address,
        tx_message :txn :afrz AS asset_freeze,
        tx_message :txn :votekey :: text AS participation_key,
        tx_message :txn :selkey :: text AS vrf_public_key,
        tx_message :txn :votefst AS vote_first,
        tx_message :txn :votelst AS vote_last,
        tx_message :txn :votekd AS vote_keydilution,
        tx_message :txn :rcv :: text AS receiver,
        tx_message :txn :asnd :: text AS asset_sender,
        tx_message :txn :arcv :: text AS asset_receiver,
        tx_message :txn :aamt AS asset_amount,
        tx_message :txn :xaid AS asset_transferred,
        tx_message :txn :amt AS amount,
        _inserted_timestamp
    FROM
        {{ ref('silver__transaction') }}

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
                dim_block_id = {{ dbt_utils.surrogate_key(
                    ['null']
                ) }}
                OR dim_account_id__tx_sender = {{ dbt_utils.surrogate_key(
                    ['null']
                ) }}
                OR dim_asset_id = {{ dbt_utils.surrogate_key(
                    ['null']
                ) }}
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
        {{ dbt_utils.surrogate_key(
            ['null']
        ) }}
    ) AS dim_block_id,
    b.block_timestamp,
    intra,
    tx_group_id,
    tx_id,
    inner_tx,
    COALESCE(
        da.dim_account_id,
        {{ dbt_utils.surrogate_key(
            ['null']
        ) }}
    ) AS dim_account_id__tx_sender,
    da.address AS tx_sender,
    COALESCE(
        dim_asset_id,
        {{ dbt_utils.surrogate_key(
            ['null']
        ) }}
    ) AS dim_asset_id,
    fee,
    COALESCE(
        dim_transaction_type_id,
        {{ dbt_utils.surrogate_key(
            ['null']
        ) }}
    ) AS dim_transaction_type_id,
    tx_message,
    extra,
    COALESCE(
        rec.dim_account_id,
        {{ dbt_utils.surrogate_key(
            ['null']
        ) }}
    ) AS dim_account_id__receiver,
    rec.address AS receiver,
    COALESCE(
        a_snd.dim_account_id,
        {{ dbt_utils.surrogate_key(
            ['null']
        ) }}
    ) AS dim_account_id__asset_sender,
    a_snd.address AS asset_sender,
    COALESCE(
        a_rec.dim_account_id,
        {{ dbt_utils.surrogate_key(
            ['null']
        ) }}
    ) AS dim_account_id__asset_receiver,
    a_rec.address AS asset_receiver,
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
    LEFT JOIN {{ ref('core__dim_block') }}
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
