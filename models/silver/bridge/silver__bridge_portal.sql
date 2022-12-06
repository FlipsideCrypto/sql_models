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
        _inserted_timestamp
    FROM
        {{ ref('silver__transaction') }}
    WHERE
        tx_type IN(
            'appl',
            'axfer',
            'pay'
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
app_calls AS (
    SELECT
        DISTINCT tx_group_id
    FROM
        base
    WHERE
        tx_type = 'appl'
        AND app_id = 842126029
),
fin AS (
    SELECT
        A.block_id,
        A.intra,
        A.tx_id,
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
        sender,
        asset_receiver,
        CASE
            inner_tx
            WHEN TRUE THEN 'inbound'
            WHEN FALSE THEN 'outbound'
        END direction,
        A._inserted_timestamp
    FROM
        base A
        INNER JOIN app_calls b
        ON A.tx_group_id = b.tx_group_id
        LEFT JOIN {{ ref('silver__asset') }}
        sa
        ON A.asset_id = sa.asset_id
    WHERE
        A.tx_type IN(
            'axfer',
            'pay'
        )
        AND amount IS NOT NULL
        AND NOT (
            A.asset_id = 0
            AND A.amount IN(
                100000,
                1002000
            )
        )
)
SELECT
    block_id,
    intra,
    tx_id,
    asset_id,
    amount,
    CASE
        WHEN direction = 'inbound' THEN asset_receiver
        WHEN direction = 'outbound' THEN sender
    END AS bridger_address,
    CASE
        WHEN direction = 'inbound' THEN sender
        WHEN direction = 'outbound' THEN asset_receiver
    END AS bridge_address,
    direction,
    _inserted_timestamp
FROM
    fin
