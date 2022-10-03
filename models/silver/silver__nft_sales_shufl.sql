{{ config(
    materialized = 'incremental',
    unique_key = '_unique_key',
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH base_tx AS (

    SELECT
        block_id,
        tx_group_id,
        app_id,
        sender,
        tx_message,
        amount,
        asset_amount,
        asset_id,
        tx_type,
        COALESCE(
            asset_receiver,
            receiver
        ) AS asset_receiver,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__transaction') }}
    WHERE
        block_id > 21940893
        AND tx_type IN (
            'appl',
            'axfer',
            'pay'
        )

{% if is_incremental() %}
AND _INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
) - INTERVAL '4 HOURS'
{% endif %}
),
purchases AS (
    --this is purchases and delists
    SELECT
        block_id,
        A.tx_group_id,
        A.app_id,
        A.sender AS purchaser,
        _INSERTED_TIMESTAMP
    FROM
        base_tx A,
        LATERAL FLATTEN(
            input => A.tx_message :dt :gd
        ) b,
        LATERAL FLATTEN(
            input => b.value
        ) C
    WHERE
        tx_type = 'appl'
        AND TRY_BASE64_DECODE_STRING(
            b.key
        ) = 'global_list_status'
        AND C.key = 'ui'
        AND C.value :: STRING = 2 {# AND app_id <> 833100004 #}
),
purchase_amount AS (
    SELECT
        -- only true purchases will have a payment transaction
        A.tx_group_id,
        SUM(
            A.amount
        ) amount
    FROM
        base_tx A
        JOIN purchases b
        ON A.tx_group_id = b.tx_group_id
        AND A.sender = b.purchaser
    WHERE
        tx_type = 'pay'
    GROUP BY
        A.tx_group_id
),
nft_id AS (
    SELECT
        A.tx_group_id,
        A.asset_id,
        A.asset_amount
    FROM
        base_tx A
        JOIN purchases b
        ON A.tx_group_id = b.tx_group_id
        AND A.asset_receiver = b.purchaser
    WHERE
        tx_type = 'axfer'
        AND A.asset_amount > 0
)
SELECT
    A.block_id,
    A.tx_group_id,
    A.purchaser,
    A.app_id,
    C.asset_id AS nft_asset_id,
    CASE
        WHEN d.decimals > 0 THEN C.asset_amount :: FLOAT / pow(
            10,
            d.decimals
        )
        WHEN NULLIF(
            d.decimals,
            0
        ) IS NULL THEN C.asset_amount :: FLOAT
    END AS number_of_nfts,
    b.amount :: FLOAT / pow(
        10,
        6
    ) AS total_sales_amount,
    concat_ws(
        '-',
        A.block_id :: STRING,
        A.tx_group_id :: STRING,
        C.asset_id :: STRING
    ) AS _unique_key,
    A._INSERTED_TIMESTAMP
FROM
    purchases A
    JOIN purchase_amount b
    ON A.tx_group_id = b.tx_group_id
    JOIN nft_id C
    ON A.tx_group_id = C.tx_group_id
    LEFT JOIN {{ ref('silver__asset') }}
    d
    ON C.asset_id = d.asset_id {# WHERE
    COALESCE(
        C.asset_id,
        0
    ) <> 833083428 #}
