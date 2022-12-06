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
        tx_group_id,
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
        app_id,
        tx_type,
        inner_tx,
        tx_message,
        _inserted_timestamp
    FROM
        {{ ref('silver__transaction') }}
    WHERE
        tx_type IN(
            'appl',
            'axfer',
            'pay'
        )
        AND block_id >= 23693885
        AND COALESCE(
            asset_receiver,
            receiver,
            ''
        ) <> 'A2GPNMIWXZDD3O3MP5UFQL6TKAZPBJEDZYHMFFITIAJZXLQH37SJZUWSZQ'

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
app_calls AS (
    SELECT
        tx_group_id,
        TRY_BASE64_DECODE_STRING(
            tx_message :txn :apaa [0]
        ) :: STRING solana_address,
        TRY_BASE64_DECODE_STRING(
            tx_message :txn :apaa [2]
        ) :: STRING solana_token,
        TRY_BASE64_DECODE_STRING(
            tx_message :txn :apaa [3]
        ) :: STRING algorand_token,
        TRY_BASE64_DECODE_STRING(
            tx_message :txn :apaa [4]
        ) :: STRING description,
        TRY_BASE64_DECODE_STRING(
            tx_message :txn :apaa [5]
        ) :: STRING solana_tx_id,
        CASE
            WHEN description LIKE '%release' THEN 'inbound'
            WHEN description LIKE '%deposit' THEN 'outbound'
        END AS direction
    FROM
        base
    WHERE
        tx_type = 'appl'
        AND app_id = 813301700
        AND description NOT LIKE '%refund' -- memo states this is for a failed transaction
)
SELECT
    A.block_id,
    A.intra,
    A.tx_id,
    A.asset_id,
    A.amount,
    CASE
        direction
        WHEN 'inbound' THEN asset_receiver
        WHEN 'outbound' THEN sender
    END bridger_address,
    CASE
        direction
        WHEN 'inbound' THEN sender
        WHEN 'outbound' THEN asset_receiver
    END bridge_address,
    direction,
    solana_address,
    solana_token,
    algorand_token,
    description,
    NULLIF(
        solana_tx_id,
        'null'
    ) AS solana_tx_id,
    CASE
        WHEN tx_message :txn :note IS NOT NULL THEN udf_base64_decode(
            tx_message :txn :note
        )
    END note,
    A._inserted_timestamp
FROM
    base A
    INNER JOIN app_calls b
    ON A.tx_group_id = b.tx_group_id
WHERE
    A.tx_type IN(
        'axfer',
        'pay'
    )
    AND amount IS NOT NULL
