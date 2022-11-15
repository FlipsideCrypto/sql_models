{{ config(
    materialized = 'incremental',
    unique_key = '_unique_key',
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH mints AS (

    SELECT
        block_id,
        tx_group_id,
        asset_receiver AS purchaser,
        b.asset_id AS nft_asset_id,
        {# CASE WHEN drop_number =  AS total_sales_amount, #}
        NULL :: FLOAT AS total_sales_amount,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__transaction') }} A
        JOIN {{ ref('silver__asset') }}
        b
        ON A.asset_id = b.asset_id
        LEFT JOIN {{ ref('silver__nft_metadata_fifa') }} C
        ON A.asset_id = C.nft_asset_id
    WHERE
        b.creator_address = 'X6MNR4AVJQEMJRHAPZ6F4O4SVDIYN67ZRMD2O3ULPY4QFMANQNZOEYHODE'
        AND asset_sender = 'X6MNR4AVJQEMJRHAPZ6F4O4SVDIYN67ZRMD2O3ULPY4QFMANQNZOEYHODE'
        AND asset_sender <> A.asset_receiver
        AND A.asset_receiver <> 'X6MNR4AVJQEMJRHAPZ6F4O4SVDIYN67ZRMD2O3ULPY4QFMANQNZOEYHODE'
        AND COALESCE(
            A.amount,
            A.asset_amount
        ) > 0
        AND b.asset_Name NOT LIKE 'test%'
        AND block_id > 23612869
        AND tx_type = 'axfer'

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
SECOND AS (
    SELECT
        block_id,
        tx_group_id,
        asset_receiver AS purchaser,
        b.asset_id AS nft_asset_id,
        {# CASE WHEN drop_number =  AS total_sales_amount, #}
        NULL :: FLOAT AS total_sales_amount,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__transaction') }} A
        JOIN {{ ref('silver__asset') }}
        b
        ON A.asset_id = b.asset_id
        LEFT JOIN {{ ref('silver__nft_metadata_fifa') }} C
        ON A.asset_id = C.nft_asset_id
    WHERE
        b.creator_address = 'X6MNR4AVJQEMJRHAPZ6F4O4SVDIYN67ZRMD2O3ULPY4QFMANQNZOEYHODE'
        AND asset_sender <> 'X6MNR4AVJQEMJRHAPZ6F4O4SVDIYN67ZRMD2O3ULPY4QFMANQNZOEYHODE'
        AND asset_sender <> A.asset_receiver
        AND A.asset_receiver <> 'X6MNR4AVJQEMJRHAPZ6F4O4SVDIYN67ZRMD2O3ULPY4QFMANQNZOEYHODE'
        AND COALESCE(
            A.amount,
            A.asset_amount
        ) > 0
        AND b.asset_Name NOT LIKE 'test%'
        AND block_id > 23612869
        AND tx_type = 'axfer'

{% if is_incremental() %}
AND (
    A._INSERTED_TIMESTAMP >= (
        SELECT
            MAX(
                _INSERTED_TIMESTAMP
            )
        FROM
            {{ this }}
    ) - INTERVAL '4 HOURS'
    OR tx_group_id IN (
        SELECT
            tx_group_id
        FROM
            {{ this }}
        WHERE
            TYPE = 'secondary'
            AND total_sales_amount IS NULL
    )
)
{% endif %}
)
SELECT
    block_id,
    tx_group_id,
    purchaser,
    A.nft_asset_id,
    1 AS number_of_nfts,
    total_sales_amount,
    CASE
        b.drop_number
        WHEN 1 THEN 4.99 / 3
        WHEN 2 THEN 4.99 / 3
        WHEN 3 THEN 9.99 / 3
    END AS total_sales_amount_USD,
    'mint' AS TYPE,
    concat_ws(
        '-',
        block_id :: STRING,
        tx_group_id :: STRING,
        A.nft_asset_id :: STRING
    ) AS _unique_key,
    _inserted_timestamp
FROM
    mints A
    LEFT JOIN {{ ref('silver__nft_metadata_fifa') }}
    b
    ON A.nft_asset_id = b.nft_asset_id
UNION ALL
SELECT
    A.block_id,
    tx_group_id,
    purchaser,
    A.nft_asset_id,
    1 AS number_of_nfts,
    A.total_sales_amount,
    C.amount / 100 AS total_sales_amount_USD,
    'secondary' AS TYPE,
    concat_ws(
        '-',
        A.block_id :: STRING,
        tx_group_id :: STRING,
        A.nft_asset_id :: STRING
    ) AS _unique_key,
    A._inserted_timestamp
FROM
    SECOND A
    JOIN {{ ref('silver__block') }}
    b
    ON A.block_id = b.block_id
    LEFT JOIN {{ ref('silver__nft_fifa_collect_secondary') }} C
    ON A.nft_asset_id = C.nft_asset_id
    AND b.block_date = C.purchase_timestamp :: DATE
    AND A.purchaser = C.recipient_address
