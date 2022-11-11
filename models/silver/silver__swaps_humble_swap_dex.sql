{{ config(
    materialized = 'incremental',
    unique_key = '_unique_key',
    incremental_strategy = 'merge',
    cluster_by = ['_INSERTED_TIMESTAMP::DATE']
) }}

WITH appl AS (

    SELECT
        A.block_id,
        A.intra,
        A.tx_group_id,
        A.tx_id,
        A.app_id,
        A.sender AS swapper,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__transaction') }} A
    WHERE
        A.tx_type = 'appl'
        AND A.block_id >= 20550530
        AND TRY_BASE64_DECODE_STRING(
            A.tx_message :txn :note :: STRING
        ) = 'Reach 0.1.10'
        AND TO_ARRAY(
            BASE64_DECODE_BINARY(
                A.tx_message :txn :apaa [1] :: STRING
            )
        ) [0] = '03'

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
trans AS (
    SELECT
        A.tx_group_id,
        A.tx_id,
        COALESCE(
            A.asset_sender,
            A.sender
        ) AS asset_sender,
        A.asset_id,
        COALESCE(
            A.amount,
            A.asset_amount
        ) AS amount,
        COALESCE(
            A.asset_receiver,
            receiver
        ) AS asset_receiver
    FROM
        {{ ref('silver__transaction') }} A
    WHERE
        tx_type IN (
            'pay',
            'axfer'
        )
        AND A.block_id >= 20550530
        AND COALESCE(
            A.amount,
            A.asset_amount
        ) > 0

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
hs_tx_group_ids AS (
    SELECT
        A.block_id,
        A.intra,
        A.tx_group_id,
        A.tx_id,
        A.app_id,
        A.swapper,
        A._INSERTED_TIMESTAMP
    FROM
        appl A
        JOIN trans b
        ON b.tx_group_id = A.tx_group_id
    GROUP BY
        A.block_id,
        A.intra,
        A.tx_group_id,
        A.tx_id,
        A.app_id,
        A.swapper,
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
        A.asset_receiver,
        CASE
            WHEN asset_sender <> b.swapper THEN asset_sender
        END AS pool_address
    FROM
        trans A
        JOIN hs_tx_group_ids b
        ON A.tx_group_id = b.tx_group_id
),
normal_swaps AS (
    SELECT
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
        A.block_id,
        A.intra,
        A.tx_group_id,
        A.tx_id,
        A.app_id,
        A.swapper,
        A._INSERTED_TIMESTAMP
    FROM
        appl A
        JOIN trans b
        ON b.tx_id = A.tx_id
        LEFT JOIN hs_tx_group_ids C
        ON A.tx_group_id = C.tx_group_id
    WHERE
        C.tx_group_id IS NULL
        AND (
            b.asset_receiver IN (
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
GROUP BY
    A.block_id,
    A.intra,
    A.tx_group_id,
    A.tx_id,
    A.app_id,
    A.swapper,
    A._INSERTED_TIMESTAMP
HAVING
    COUNT(1) = 2
),
odd_hs_transfers AS (
    SELECT
        A.tx_group_id,
        A.tx_id,
        A.asset_sender,
        A.asset_id,
        A.amount,
        A.asset_receiver,
        CASE
            WHEN rec.pool_address IS NOT NULL THEN TRUE
            ELSE FALSE
        END rec_is_pool
    FROM
        trans A
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
ON A.asset_receiver = rec.pool_address
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
    (
        rec.pool_address IS NOT NULL
        OR sndr.pool_address IS NOT NULL
    )
),
odd_swaps AS (
    SELECT
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
),
FINAL AS (
    SELECT
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
)
SELECT
    block_id,
    intra,
    tx_group_id,
    tx_id,
    app_id,
    swapper,
    swap_from_asset_id,
    CASE
        WHEN swap_from_asset_id = 0 THEN A.swap_from_amount / pow(
            10,
            6
        )
        WHEN af.decimals > 0 THEN A.swap_from_amount / pow(
            10,
            af.decimals
        )
        ELSE A.swap_from_amount
    END :: FLOAT AS swap_from_amount,
    pool_address,
    swap_to_asset_id,
    CASE
        WHEN swap_to_asset_id = 0 THEN A.swap_to_amount / pow(
            10,
            6
        )
        WHEN ato.decimals > 0 THEN A.swap_to_amount / pow(
            10,
            ato.decimals
        )
        ELSE A.swap_to_amount
    END :: FLOAT AS swap_to_amount,
    _unique_key,
    A._INSERTED_TIMESTAMP,
    TYPE
FROM
    FINAL A
    LEFT JOIN {{ ref('silver__asset') }}
    af
    ON af.asset_id = A.swap_from_asset_id
    LEFT JOIN {{ ref('silver__asset') }}
    ato
    ON ato.asset_id = A.swap_to_asset_id
