{{ config(
    materialized = 'incremental',
    unique_key = '_unique_key',
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH ab2_sales AS(

    SELECT
        DISTINCT block_id,
        tx_group_id,
        sender,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__transaction') }}
    WHERE
        tx_type = 'appl'
        AND TRY_BASE64_DECODE_STRING(
            tx_message :txn :note :: STRING
        ) = 'ab2.gallery'

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
nft_transfer AS(
    SELECT
        ab2.block_id,
        ab2.tx_group_id,
        ab2.sender AS buyer,
        nft.asset_amount AS number_of_assets,
        ast.asset_id,
        ab2._INSERTED_TIMESTAMP,
        decimals
    FROM
        ab2_sales ab2
        JOIN {{ ref('silver__transaction') }}
        nft
        ON ab2.tx_group_id = nft.tx_group_id
        AND ab2.sender = nft.asset_receiver
        JOIN {{ ref('silver__asset') }}
        ast
        ON nft.asset_id = ast.asset_id
    WHERE
        tx_type = 'axfer'
        AND asset_amount > 0

{% if is_incremental() %}
AND ab2._INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
) - INTERVAL '4 HOURS'
{% endif %}
)
SELECT
    nft.block_id,
    nft.tx_group_id,
    nft.buyer AS purchaser,
    nft.asset_id AS nft_asset_id,
    SUM(
        amount :: FLOAT / pow(
            10,
            6
        )
    ) AS total_sales_amount,
    CASE
        WHEN nft.decimals > 0 THEN number_of_assets :: FLOAT / pow(
            10,
            nft.decimals
        )
        WHEN NULLIF(
            nft.decimals,
            0
        ) IS NULL THEN number_of_assets :: FLOAT
    END AS number_of_nfts,
    concat_ws(
        '-',
        nft.block_id :: STRING,
        nft.tx_group_id :: STRING,
        nft.asset_id :: STRING
    ) AS _unique_key,
    nft._INSERTED_TIMESTAMP
FROM
    nft_transfer nft
    JOIN {{ ref('silver__transaction') }}
    pay
    ON nft.tx_group_id = pay.tx_group_id
    AND nft.buyer = pay.sender
WHERE
    tx_type = 'pay'
GROUP BY
    nft.block_id,
    nft.tx_group_id,
    purchaser,
    nft.asset_id,
    number_of_nfts,
    _unique_key,
    nft._INSERTED_TIMESTAMP
