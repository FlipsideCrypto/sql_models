{{ config(
    materialized = 'incremental',
    unique_key = 'tx_group_id',
    incremental_strategy = 'merge',
    tags = ['snowflake', 'algorand', 'transactions', 'algorand_swaps']
) }}

WITH tinymanapp AS(

    SELECT
        algorand_decode_b64_addr(
            tx_message :txn :apat [0] :: STRING
        ) AS swapper,
        tx_group_id AS tx_group_id,
        block_timestamp,
        block_id,
        tx_id,
        intra AS app_intra,
        app_id AS app_id
    FROM
        {{ ref('silver_algorand__application_call_transaction') }}
    WHERE
        (
            app_id = 350338509
            OR app_id = 552635992
        )
        AND TRY_BASE64_DECODE_STRING(
            tx_message :txn :apaa [0] :: STRING
        ) = 'swap' ---get pool asset_id as well
),
sender_pay AS (
    SELECT
        pt.tx_group_id,
        pt.sender AS swapper,
        0 AS from_asset_id,
        amount AS swap_from_amount
    FROM
        tinymanapp ta
        LEFT JOIN {{ ref('silver_algorand__payment_transaction') }}
        pt
        ON pt.tx_group_id = ta.tx_group_id
        AND pt.sender = ta.swapper
        AND pt.intra > ta.app_intra
    WHERE
        pt.tx_group_id IS NOT NULL
),
sender_asset AS (
    SELECT
        pt.tx_group_id,
        pt.sender AS swapper,
        pt.asset_id AS from_asset_id,
        asset_amount / pow(
            10,
            A.decimals
        ) AS swap_from_amount
    FROM
        tinymanapp ta
        LEFT JOIN {{ ref('silver_algorand__asset_transfer_transaction') }}
        pt
        ON pt.tx_group_id = ta.tx_group_id
        AND pt.sender = ta.swapper
        AND pt.intra > ta.app_intra
        LEFT JOIN {{ ref('silver_algorand__asset') }} A
        ON pt.asset_id = A.asset_id
    WHERE
        pt.tx_group_id IS NOT NULL
),
receiver_pay AS(
    SELECT
        pt.tx_group_id,
        pt.sender AS pool_address,
        0 AS to_asset_id,
        amount AS swap_to_amount
    FROM
        tinymanapp ta
        LEFT JOIN {{ ref('silver_algorand__payment_transaction') }}
        pt
        ON pt.tx_group_id = ta.tx_group_id
    WHERE
        ta.tx_group_id IS NOT NULL
        AND pt.receiver = ta.swapper
        AND pt.intra > app_intra
        AND pt.tx_group_id IS NOT NULL
),
receiver_asset AS (
    SELECT
        pt.tx_group_id,
        pt.sender AS pool_address,
        pt.asset_id AS to_asset_id,
        asset_amount / pow(
            10,
            A.decimals
        ) AS swap_to_amount
    FROM
        tinymanapp ta
        LEFT JOIN {{ ref('silver_algorand__asset_transfer_transaction') }}
        pt
        ON pt.tx_group_id = ta.tx_group_id
        LEFT JOIN {{ ref('silver_algorand__asset') }} A
        ON pt.asset_id = A.asset_id
    WHERE
        ta.tx_group_id IS NOT NULL
        AND pt.asset_receiver = ta.swapper
        AND pt.intra > app_intra
        AND pt.tx_group_id IS NOT NULL
),
all_sender AS (
    SELECT
        *
    FROM
        sender_pay
    UNION
    SELECT
        *
    FROM
        sender_asset
),
all_receiver AS(
    SELECT
        *
    FROM
        receiver_pay
    UNION
    SELECT
        *
    FROM
        receiver_asset
)
SELECT
    ta.block_timestamp,
    ta.block_id AS block_id,
    ta.app_intra AS intra,
    ta.tx_group_id AS tx_group_id,
    app_id,
    als.swapper,
    als.from_asset_id,
    als.swap_from_amount,
    ars.pool_address AS pool_address,
    ars.to_asset_id,
    ars.swap_to_amount
FROM
    tinymanapp ta
    LEFT JOIN all_sender als
    ON ta.tx_group_id = als.tx_group_id
    LEFT JOIN all_receiver ars
    ON ta.tx_group_id = ars.tx_group_id
WHERE
    ars.tx_group_id IS NOT NULL
    AND als.tx_group_id IS NOT NULL
