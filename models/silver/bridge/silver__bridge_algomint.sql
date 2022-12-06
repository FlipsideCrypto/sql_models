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
        asset_amount AS amount,
        asset_receiver,
        sender,
        _inserted_timestamp
    FROM
        {{ ref('silver__transaction') }}
    WHERE
        tx_type = 'axfer'
        AND asset_amount IS NOT NULL
        AND (
            sender = 'ETGSQKACKC56JWGMDAEP5S2JVQWRKTQUVKCZTMPNUGZLDVCWPY63LSI3H4'
            OR asset_receiver = 'ETGSQKACKC56JWGMDAEP5S2JVQWRKTQUVKCZTMPNUGZLDVCWPY63LSI3H4'
        )
        AND NOT (
            sender = 'ETGSQKACKC56JWGMDAEP5S2JVQWRKTQUVKCZTMPNUGZLDVCWPY63LSI3H4'
            AND asset_receiver = 'ETGSQKACKC56JWGMDAEP5S2JVQWRKTQUVKCZTMPNUGZLDVCWPY63LSI3H4'
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
algomint AS (
    SELECT
        block_id,
        intra,
        tx_id,
        asset_id,
        amount,
        asset_receiver bridger_address,
        sender AS bridge_address,
        'inbound' AS direction,
        _inserted_timestamp
    FROM
        base
    WHERE
        sender = 'ETGSQKACKC56JWGMDAEP5S2JVQWRKTQUVKCZTMPNUGZLDVCWPY63LSI3H4'
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
        _inserted_timestamp
    FROM
        base
    WHERE
        asset_receiver = 'ETGSQKACKC56JWGMDAEP5S2JVQWRKTQUVKCZTMPNUGZLDVCWPY63LSI3H4'
)
SELECT
    block_id,
    intra,
    tx_id,
    A.asset_id,
    amount,
    bridger_address,
    bridge_address,
    direction,
    A._inserted_timestamp
FROM
    algomint A
