{{ config(
    materialized = 'incremental',
    unique_key = '_unique_key',
    incremental_strategy = 'merge'
) }}

WITH asset_ids AS (

    SELECT
        asset_id,
        tx_group_id,
        sender AS bridge_account
    FROM
        {{ ref('silver_algorand__asset_configuration_transaction') }}
    WHERE
        asset_parameters :un :: STRING = 'NFD'
        AND asset_id <> '813293109'
        AND block_timestamp :: DATE >= '2022-06-01'
),
nfdadmin_minted AS (
    SELECT
        A.tx_group_id,
        b.asset_id,
        block_id,
        block_timestamp,
        sender AS purchaser,
        A._inserted_timestamp,
        COUNT(1) xcount
    FROM
        {{ ref('silver_algorand__payment_transaction') }} A
        JOIN asset_ids b
        ON A.tx_group_id = b.tx_group_id
    WHERE
        A.block_timestamp >= '2022-06-01'
        AND A.sender = 'ABHE544MXL2CWMIZONAIUBNVELWYMKYKWBDNRLOEFQJN3LNF2ZWSMDEKBQ'
    GROUP BY
        A.tx_group_id,
        b.asset_id,
        block_id,
        block_timestamp,
        A.sender,
        A._inserted_timestamp
    HAVING
        COUNT(1) = 4
),
minted_nfts AS (
    SELECT
        A.block_timestamp,
        A.block_id,
        A.tx_group_id,
        A.sender AS purchaser,
        b.asset_id,
        A._inserted_timestamp,
        SUM(
            C.amount
        ) amount
    FROM
        {{ ref('silver_algorand__payment_transaction') }} A
        JOIN asset_ids b
        ON A.tx_group_id = b.tx_group_id
        JOIN {{ ref('silver_algorand__payment_transaction') }} C
        ON A.tx_group_id = C.tx_group_id
        AND A.sender = C.sender
    WHERE
        A.block_timestamp >= '2022-06-01'
        AND C.block_timestamp >= '2022-06-01'
        AND A.receiver = 'ABHE544MXL2CWMIZONAIUBNVELWYMKYKWBDNRLOEFQJN3LNF2ZWSMDEKBQ'

{% if is_incremental() %}
AND A._INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
) - INTERVAL '4 HOURS'
AND C._INSERTED_TIMESTAMP >= (
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
    A.tx_group_id,
    A.sender,
    b.asset_id,
    A._inserted_timestamp
),
all_mint_claims AS (
    SELECT
        A.tx_group_id,
        A.asset_id,
        A.block_timestamp,
        A.block_id,
        A.asset_receiver AS purchaser,
        A._inserted_timestamp
    FROM
        {{ ref('silver_algorand__asset_transfer_transaction') }} A
        JOIN asset_ids b
        ON A.asset_id = b.asset_id
        AND A.asset_sender = b.bridge_account
    WHERE
        A.block_timestamp >= '2022-06-01'
        AND A.asset_amount > 0
),
xfers_base AS (
    SELECT
        A.tx_group_id,
        A.block_id,
        tx_id,
        A.block_timestamp,
        asset_sender AS seller,
        asset_receiver AS purchaser,
        A.asset_id,
        A._inserted_timestamp
    FROM
        {{ ref('silver_algorand__asset_transfer_transaction') }} A
        JOIN asset_ids b
        ON A.asset_id = b.asset_id
    WHERE
        A.block_timestamp >= '2022-06-01'
        AND asset_amount > 0

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
        A.block_id,
        A.tx_id,
        A.block_timestamp,
        A.seller,
        A.purchaser,
        A.asset_id,
        A._inserted_timestamp
    FROM
        xfers_base A
        JOIN asset_ids b
        ON A.asset_id = b.asset_id
        LEFT JOIN all_mint_claims C
        ON A.tx_group_id = C.tx_group_id
    WHERE
        C.tx_group_id IS NULL
),
xfers_pay AS (
    SELECT
        A.tx_group_id,
        SUM(amount) amount
    FROM
        {{ ref('silver_algorand__payment_transaction') }} A
        JOIN xfers_base b
        ON A.tx_group_id = b.tx_group_id
        AND A.sender = b.purchaser
    WHERE
        A.block_timestamp >= '2022-06-01'

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
    A.tx_group_id
),
FINAL AS (
    SELECT
        block_timestamp,
        block_id,
        A.tx_group_id,
        purchaser,
        asset_id AS nft_asset_id,
        1 AS number_of_nfts,
        CASE
            WHEN b.tx_group_id = 'wHGynFHbMjJtK0Pus7V91bSaORPg+ZvGkzSrBqbbA2g=' THEN 1408
            ELSE amount
        END AS total_sales_amount,
        'secondary and transfers' TYPE,
        A._inserted_timestamp
    FROM
        xfers A
        JOIN xfers_pay b
        ON A.tx_group_id = b.tx_group_id -- order by 2
    UNION ALL
    SELECT
        block_timestamp,
        block_id,
        tx_group_id,
        purchaser,
        asset_id,
        1,
        amount,
        'primary' TYPE,
        _inserted_timestamp
    FROM
        minted_nfts
    UNION ALL
    SELECT
        b.block_timestamp,
        b.block_id,
        b.tx_group_id,
        b.purchaser,
        A.asset_id,
        1,
        amount,
        'curated' TYPE,
        b._inserted_timestamp
    FROM
        nfdadmin_minted A
        JOIN all_mint_claims b
        ON A.asset_id = b.asset_id
        JOIN xfers_pay C
        ON b.tx_group_id = C.tx_group_id
    WHERE
        b.tx_group_id <> 'wHGynFHbMjJtK0Pus7V91bSaORPg+ZvGkzSrBqbbA2g='
)
SELECT
    block_timestamp,
    block_id,
    tx_group_id,
    purchaser,
    nft_asset_id,
    number_of_nfts,
    total_sales_amount,
    TYPE,
    concat_ws(
        '-',
        block_id :: STRING,
        tx_group_id :: STRING,
        nft_asset_id :: STRING
    ) AS _unique_key,
    _inserted_timestamp
FROM
    FINAL
