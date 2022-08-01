{{ config(
    materialized = 'incremental',
    unique_key = '_unique_key',
    incremental_strategy = 'merge'
) }}

WITH ab2_sales AS(

    SELECT
        DISTINCT block_id,
        block_timestamp,
        tx_group_id,
        tx_sender,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('core__fact_transaction') }}
    WHERE
        dim_transaction_type_id = '63469c3c4f19f07c737127a117296de4'
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
        ab2.block_timestamp,
        ab2.tx_group_id,
        ab2.tx_sender AS buyer,
        nft.asset_amount AS number_of_assets,
        ast.asset_id,
        ab2._INSERTED_TIMESTAMP,
        decimals
    FROM
        ab2_sales ab2
        JOIN {{ ref('core__fact_transaction') }}
        nft
        ON ab2.tx_group_id = nft.tx_group_id
        AND ab2.tx_sender = nft.asset_receiver
        JOIN {{ ref('core__dim_asset') }}
        ast
        ON nft.dim_asset_id = ast.dim_asset_id
    WHERE
        dim_transaction_type_id = 'c495d86d106bb9c67e5925d952e553f2'
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
    nft.block_timestamp,
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
    JOIN {{ ref('core__fact_transaction') }}
    pay
    ON nft.tx_group_id = pay.tx_group_id
    AND nft.buyer = pay.tx_sender
WHERE
    dim_transaction_type_id = 'b02a45a596bfb86fe2578bde75ff5444'
GROUP BY
    nft.block_id,
    nft.block_timestamp,
    nft.tx_group_id,
    purchaser,
    nft.asset_id,
    number_of_nfts,
    _unique_key,
    nft._INSERTED_TIMESTAMP
