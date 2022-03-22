{{ config(
    materialized = 'incremental',
    unique_key = '_unique_key',
    incremental_strategy = 'merge',
    tags = ['snowflake', 'algorand', 'transactions', 'algorand_swaps']
) }}

WITH tinymanapp AS(

    SELECT
        algorand_decode_b64_addr(
            tx_message :txn :apat [0] :: STRING
        ) AS swapper,
        tx_group_id AS tx_group_id,
        _INSERTED_TIMESTAMP,
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
        ) = 'swap'
),
sender_pay AS (
    SELECT
        pt.tx_group_id,
        pt.sender AS swapper,
        'ALGO' AS from_asset_name,
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
        A.asset_name AS from_asset_name,
        pt.asset_id AS from_asset_id,
        CASE
            WHEN A.decimals > 0 THEN asset_amount :: FLOAT / pow(
                10,
                A.decimals
            )
            ELSE asset_amount :: FLOAT
        END AS swap_from_amount
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
        'ALGO' AS to_asset_name,
        0 AS to_asset_id,
        ZEROIFNULL(amount) AS swap_to_amount
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
        A.asset_name AS to_asset_name,
        pt.asset_id AS to_asset_id,
        CASE
            WHEN A.decimals > 0 THEN ZEROIFNULL(
                asset_amount :: FLOAT / pow(
                    10,
                    A.decimals
                )
            )
            ELSE ZEROIFNULL(
                asset_amount
            )
        END AS swap_to_amount
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
    app_intra AS intra,
    ta.tx_group_id AS tx_group_id,
    app_id,
    als.swapper,
    als.from_asset_id AS swap_from_asset_id,
    als.swap_from_amount :: FLOAT AS swap_from_amount,
    ars.pool_address AS pool_address,
    ars.to_asset_id :: FLOAT AS swap_to_asset_id,
    ars.swap_to_amount AS swap_to_amount,
    concat_ws(
        '-',
        ta.block_id :: STRING,
        app_intra :: STRING
    ) AS _unique_key,
    ta._INSERTED_TIMESTAMP
FROM
    tinymanapp ta
    LEFT JOIN all_sender als
    ON ta.tx_group_id = als.tx_group_id
    LEFT JOIN all_receiver ars
    ON ta.tx_group_id = ars.tx_group_id
WHERE
    ars.tx_group_id IS NOT NULL
    AND als.tx_group_id IS NOT NULL

{% if is_incremental() %}
AND ta._INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
)
{% endif %}
