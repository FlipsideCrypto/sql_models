{{ config(
    materialized = 'incremental',
    unique_key = '_unique_key',
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH tx_app_call AS (

    SELECT
        *
    FROM
        {{ ref('silver__transaction') }}
    WHERE
        tx_type = 'appl'

{% if is_incremental() %}
AND _INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
) - INTERVAL '48 HOURS'
{% endif %}
),
tx_pay AS (
    SELECT
        *
    FROM
        {{ ref('silver__transaction') }}
    WHERE
        tx_type = 'pay'

{% if is_incremental() %}
AND _INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
) - INTERVAL '48 HOURS'
{% endif %}
),
tx_a_tfer AS (
    SELECT
        pt.*,
        A.asset_name,
        A.decimals
    FROM
        {{ ref('silver__transaction') }}
        pt
        JOIN {{ ref('silver__asset') }} A
        ON pt.asset_id = A.asset_id
    WHERE
        tx_type = 'axfer'

{% if is_incremental() %}
AND pt._INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
) - INTERVAL '48 HOURS'
{% endif %}
),
tinymanapp AS(
    SELECT
        algorand_decode_b64_addr(
            tx_message :txn :apat [0] :: STRING
        ) AS swapper,
        tx_group_id AS tx_group_id,
        _INSERTED_TIMESTAMP,
        block_id,
        tx_id,
        intra AS app_intra,
        app_id
    FROM
        tx_app_call
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
        amount :: FLOAT / pow(
            10,
            6
        ) AS swap_from_amount
    FROM
        tinymanapp ta
        JOIN tx_pay pt
        ON pt.tx_group_id = ta.tx_group_id
        AND pt.sender = ta.swapper
        AND pt.intra > ta.app_intra
),
sender_asset AS (
    SELECT
        pt.tx_group_id,
        pt.sender AS swapper,
        asset_name AS from_asset_name,
        asset_id AS from_asset_id,
        CASE
            WHEN decimals > 0 THEN asset_amount :: FLOAT / pow(
                10,
                decimals
            )
            ELSE asset_amount :: FLOAT
        END AS swap_from_amount
    FROM
        tinymanapp ta
        JOIN tx_a_tfer pt
        ON pt.tx_group_id = ta.tx_group_id
        AND pt.sender = ta.swapper
        AND pt.intra > ta.app_intra
),
receiver_pay AS(
    SELECT
        pt.tx_group_id,
        pt.sender AS pool_address,
        'ALGO' AS to_asset_name,
        0 AS to_asset_id,
        ZEROIFNULL(amount) :: FLOAT / pow(
            10,
            6
        ) AS swap_to_amount
    FROM
        tinymanapp ta
        JOIN tx_pay pt
        ON pt.tx_group_id = ta.tx_group_id
        AND pt.receiver = ta.swapper
        AND pt.intra > app_intra
),
receiver_asset AS (
    SELECT
        pt.tx_group_id,
        pt.sender AS pool_address,
        asset_name AS to_asset_name,
        asset_id AS to_asset_id,
        CASE
            WHEN decimals > 0 THEN ZEROIFNULL(
                asset_amount :: FLOAT / pow(
                    10,
                    decimals
                )
            )
            ELSE ZEROIFNULL(
                asset_amount
            )
        END AS swap_to_amount
    FROM
        tinymanapp ta
        JOIN tx_a_tfer pt
        ON pt.tx_group_id = ta.tx_group_id
        AND pt.asset_receiver = ta.swapper
        AND pt.intra > app_intra
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
    JOIN all_sender als
    ON ta.tx_group_id = als.tx_group_id
    JOIN all_receiver ars
    ON ta.tx_group_id = ars.tx_group_id
