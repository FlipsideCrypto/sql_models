{{ config(
    materialized = 'incremental',
    unique_key = ['block_id','intra'],
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH base AS (

    SELECT
        block_id,
        intra,
        tx_id,
        asset_id,
        COALESCE(
            asset_amount,
            amount
        ) AS amount,
        COALESCE(
            asset_receiver,
            receiver
        ) AS asset_receiver,
        sender,
        TRY_BASE64_DECODE_STRING(
            tx_message :txn :note
        ) AS note,
        _inserted_timestamp
    FROM
        {{ ref('silver__transaction') }}
    WHERE
        tx_type IN (
            'axfer',
            'pay'
        )
        AND COALESCE(
            asset_amount,
            amount
        ) IS NOT NULL
        AND (
            sender = 'MO23TSNBDBCRKSIZI5LETMQBTIOD4QF5MONXKBZXCNBODNVREDJ3MI6IZQ'
            OR asset_receiver = 'MO23TSNBDBCRKSIZI5LETMQBTIOD4QF5MONXKBZXCNBODNVREDJ3MI6IZQ'
            OR receiver = 'MO23TSNBDBCRKSIZI5LETMQBTIOD4QF5MONXKBZXCNBODNVREDJ3MI6IZQ'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
),
milk AS (
    SELECT
        block_id,
        intra,
        tx_id,
        asset_id,
        amount,
        asset_receiver bridger_address,
        sender AS bridge_address,
        'inbound' AS direction,
        REPLACE(SPLIT_PART(note, '/', 2), 'a1:') AS milkomeda_address,
        _inserted_timestamp
    FROM
        base
    WHERE
        sender = 'MO23TSNBDBCRKSIZI5LETMQBTIOD4QF5MONXKBZXCNBODNVREDJ3MI6IZQ'
    UNION ALL
    SELECT
        block_id,
        intra,
        tx_id,
        asset_id,
        amount,
        sender bridger_address,
        asset_receiver AS bridge_address,
        'outbound' AS direction,
        REPLACE(SPLIT_PART(note, '/', 2), 'a1:u') AS milkomeda_address,
        _inserted_timestamp
    FROM
        base
    WHERE
        asset_receiver = 'MO23TSNBDBCRKSIZI5LETMQBTIOD4QF5MONXKBZXCNBODNVREDJ3MI6IZQ'
)
SELECT
    block_id,
    intra,
    tx_id,
    A.asset_id,
    CASE
        WHEN A.asset_id = 0 THEN A.amount / pow(
            10,
            6
        )
        WHEN sa.decimals > 0 THEN A.amount / pow(
            10,
            sa.decimals
        )
        ELSE A.amount
    END :: FLOAT AS amount,
    bridger_address,
    bridge_address,
    direction,
    milkomeda_address,
    A._inserted_timestamp
FROM
    milk A
    LEFT JOIN {{ ref('silver__asset') }}
    sa
    ON A.asset_id = sa.asset_id
