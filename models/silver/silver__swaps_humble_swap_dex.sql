{{ config(
    materialized = 'incremental',
    unique_key = '_unique_key',
    incremental_strategy = 'merge',
    cluster_by = ['_INSERTED_TIMESTAMP::DATE']
) }}

WITH appcall AS (

    SELECT
        A.block_id,
        A.intra,
        A.tx_group_id,
        A.app_id,
        A.sender AS swapper,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__transaction') }} A
    WHERE
        A.block_id >= 20553107
        AND tx_type = 'appl'
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
xfers AS (
    SELECT
        A.tx_group_id,
        COALESCE(
            A.asset_sender,
            A.sender
        ) AS asset_sender,
        A.asset_id,
        COALESCE(
            asset_amount,
            amount
        ) AS amount,
        A.receiver
    FROM
        {{ ref('silver__transaction') }} A
    WHERE
        A.block_id >= 20553107
        AND tx_type IN (
            'axfer',
            'pay'
        )
        AND COALESCE(asset_amount, amount) > 0

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
        A.app_id,
        A.swapper,
        A._INSERTED_TIMESTAMP
    FROM
        appcall A
        JOIN xfers b
        ON b.tx_group_id = A.tx_group_id
    GROUP BY
        A.block_id,
        A.intra,
        A.tx_group_id,
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
        A.receiver,
        CASE
            WHEN A.asset_sender <> b.swapper THEN A.asset_sender
        END AS pool_address,
        asa.decimals
    FROM
        xfers A
        JOIN hs_tx_group_ids b
        ON A.tx_group_id = b.tx_group_id
        JOIN {{ ref('silver__asset') }}
        asa
        ON A.asset_id = asa.asset_id
)
SELECT
    A.block_id,
    A.intra,
    A.tx_group_id,
    A.app_id,
    A.swapper,
    b.asset_id AS swap_from_asset_id,
    CASE
        WHEN b.decimals > 0 THEN b.amount / pow(
            10,
            b.decimals
        )
        ELSE b.amount
    END :: FLOAT AS swap_from_amount,
    C.pool_address,
    C.asset_id AS swap_to_asset_id,
    CASE
        WHEN C.decimals > 0 THEN C.amount / pow(
            10,
            C.decimals
        )
        ELSE C.amount
    END :: FLOAT AS swap_to_amount,
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
