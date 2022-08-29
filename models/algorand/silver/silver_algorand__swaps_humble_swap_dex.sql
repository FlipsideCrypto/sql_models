{{ config(
    materialized = 'incremental',
    unique_key = '_unique_key',
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH hs_tx_group_ids AS (

    SELECT
        A.block_timestamp,
        A.block_id,
        A.intra,
        A.tx_group_id,
        A.tx_id,
        A.app_id,
        A.sender AS swapper,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver_algorand__application_call_transaction') }} A
        JOIN {{ ref('silver_algorand__transfers') }}
        b
        ON b.tx_group_id = A.tx_group_id
    WHERE
        A.block_timestamp :: DATE >= '2022-04-22'
        AND b.block_timestamp :: DATE >= '2022-04-22'
        AND TRY_BASE64_DECODE_STRING(
            A.tx_message :txn :note :: STRING
        ) = 'Reach 0.1.10'
        AND TO_ARRAY(
            BASE64_DECODE_BINARY(
                A.tx_message :txn :apaa [1] :: STRING
            )
        ) [0] = '03'
        AND amount > 0

{% if is_incremental() %}
AND A._INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
) - INTERVAL '4 HOURS'
{% endif %}
GROUP BY
    A.block_timestamp,
    A.block_id,
    A.intra,
    A.tx_group_id,
    A.tx_id,
    A.app_id,
    A.sender,
    A._INSERTED_TIMESTAMP
HAVING
    COUNT(1) = 2
),
hs_transfers AS(
    SELECT
        A.tx_group_id,
        A.asset_sender,
        A.asset_id,
        A.amount,
        A.receiver,
        CASE
            WHEN A.asset_sender <> b.swapper THEN A.asset_sender
        END AS pool_address
    FROM
        {{ ref('silver_algorand__transfers') }} A
        JOIN hs_tx_group_ids b
        ON A.tx_group_id = b.tx_group_id
    WHERE
        A.block_timestamp :: DATE >= '2022-04-22'
        AND amount > 0
),
normal_swaps AS (
    SELECT
        A.block_timestamp,
        A.block_id,
        A.intra,
        A.tx_group_id,
        A.tx_id,
        A.app_id,
        A.swapper,
        b.asset_id AS swap_from_asset_id,
        b.amount :: FLOAT AS swap_from_amount,
        C.pool_address,
        C.asset_id AS swap_to_asset_id,
        C.amount :: FLOAT AS swap_to_amount,
        concat_ws(
            '-',
            A.block_id :: STRING,
            A.intra :: STRING
        ) AS _unique_key,
        _INSERTED_TIMESTAMP
    FROM
        hs_tx_group_ids A
        JOIN hs_transfers b
        ON A.tx_group_id = b.tx_group_id
        AND b.pool_address IS NULL
        JOIN hs_transfers C
        ON A.tx_group_id = C.tx_group_id
        AND C.pool_address IS NOT NULL
),
hs_tx_ids AS (
    SELECT
        A.block_timestamp,
        A.block_id,
        A.intra,
        A.tx_group_id,
        A.tx_id,
        A.app_id,
        A.sender AS swapper,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver_algorand__application_call_transaction') }} A
        JOIN {{ ref('silver_algorand__transfers') }}
        b
        ON b.tx_id = A.tx_id
        LEFT JOIN hs_tx_group_ids C
        ON A.tx_group_id = C.tx_group_id
    WHERE
        A.block_timestamp :: DATE >= '2022-04-22'
        AND b.block_timestamp :: DATE >= '2022-04-22'
        AND TRY_BASE64_DECODE_STRING(
            A.tx_message :txn :note :: STRING
        ) = 'Reach 0.1.10'
        AND TO_ARRAY(
            BASE64_DECODE_BINARY(
                A.tx_message :txn :apaa [1] :: STRING
            )
        ) [0] = '03'
        AND amount > 0
        AND C.tx_group_id IS NULL
        AND (
            receiver IN (
                SELECT
                    pool_address
                FROM
                    normal_swaps

{% if is_incremental() %}
UNION ALL
SELECT
    pool_address
FROM
    {{ this }}
{% endif %}
)
OR asset_sender IN (
    SELECT
        pool_address
    FROM
        normal_swaps

{% if is_incremental() %}
UNION ALL
SELECT
    pool_address
FROM
    {{ this }}
{% endif %}
)
)

{% if is_incremental() %}
AND A._INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
) - INTERVAL '4 HOURS'
{% endif %}
GROUP BY
    A.block_timestamp,
    A.block_id,
    A.intra,
    A.tx_group_id,
    A.tx_id,
    A.app_id,
    A.sender,
    A._INSERTED_TIMESTAMP
HAVING
    COUNT(1) = 2
),
odd_hs_transfers AS (
    SELECT
        A.block_timestamp,
        A.tx_group_id,
        A.tx_id,
        A.asset_sender,
        A.asset_id,
        A.amount,
        A.receiver,
        CASE
            WHEN rec.pool_address IS NOT NULL THEN TRUE
            ELSE FALSE
        END rec_is_pool
    FROM
        {{ ref('silver_algorand__transfers') }} A
        LEFT JOIN hs_tx_ids b
        ON A.tx_id = b.tx_id
        LEFT JOIN (
            SELECT
                DISTINCT pool_address
            FROM
                normal_swaps

{% if is_incremental() %}
UNION
SELECT
    DISTINCT pool_address
FROM
    {{ this }}
{% endif %}
) rec
ON A.receiver = rec.pool_address
LEFT JOIN (
    SELECT
        DISTINCT pool_address
    FROM
        normal_swaps

{% if is_incremental() %}
UNION
SELECT
    DISTINCT pool_address
FROM
    {{ this }}
{% endif %}
) sndr
ON A.asset_sender = sndr.pool_address
WHERE
    rec.pool_address IS NOT NULL
    OR sndr.pool_address IS NOT NULL

{% if is_incremental() %}
AND A._INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
) - INTERVAL '4 HOURS'
{% endif %}
),
odd_swaps AS (
    SELECT
        A.block_timestamp,
        A.block_id,
        A.intra,
        A.tx_group_id,
        A.tx_id,
        A.app_id,
        A.swapper,
        b.asset_id AS swap_from_asset_id,
        b.amount :: FLOAT AS swap_from_amount,
        C.asset_sender pool_address,
        C.asset_id AS swap_to_asset_id,
        C.amount :: FLOAT AS swap_to_amount,
        concat_ws(
            '-',
            A.block_id :: STRING,
            A.intra :: STRING
        ) AS _unique_key,
        _INSERTED_TIMESTAMP
    FROM
        hs_tx_ids A
        JOIN odd_hs_transfers b
        ON A.tx_id = b.tx_id
        AND b.rec_is_pool = TRUE
        JOIN odd_hs_transfers C
        ON A.tx_id = C.tx_id
        AND C.rec_is_pool = FALSE
)
SELECT
    block_timestamp,
    block_id,
    intra,
    tx_group_id,
    tx_id,
    app_id,
    swapper,
    swap_from_asset_id,
    swap_from_amount,
    pool_address,
    swap_to_asset_id,
    swap_to_amount,
    _unique_key,
    _INSERTED_TIMESTAMP,
    'normal' TYPE
FROM
    normal_swaps
UNION ALL
SELECT
    block_timestamp,
    block_id,
    intra,
    tx_group_id,
    tx_id,
    app_id,
    swapper,
    swap_from_asset_id,
    swap_from_amount,
    pool_address,
    swap_to_asset_id,
    swap_to_amount,
    _unique_key,
    _INSERTED_TIMESTAMP,
    'other' TYPE
FROM
    odd_swaps
