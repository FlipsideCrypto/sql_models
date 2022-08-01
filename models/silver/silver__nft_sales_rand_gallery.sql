{{ config(
    materialized = 'incremental',
    unique_key = '_unique_key',
    incremental_strategy = 'merge'
) }}

WITH group_tx AS (

    SELECT
        block_id,
        block_timestamp,
        tx_group_id,
        COALESCE(
            asset_sender,
            tx_sender
        ) AS asset_sender,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('core__fact_transaction') }}
    WHERE
        dim_transaction_type_id IN (
            'c495d86d106bb9c67e5925d952e553f2',
            'b02a45a596bfb86fe2578bde75ff5444'
        )
        AND receiver = 'RANDGVRRYGVKI3WSDG6OGTZQ7MHDLIN5RYKJBABL46K5RQVHUFV3NY5DUE' {# AND asset_sender != 'RANDGVRRYGVKI3WSDG6OGTZQ7MHDLIN5RYKJBABL46K5RQVHUFV3NY5DUE' #}
        AND tx_group_ID != '//bQaOEaOkBwSub8XBEk86t4wWdb6F/7fePO4fIXyho='
        AND tx_group_id IS NOT NULL

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
GROUP BY
    block_id,
    block_timestamp,
    tx_group_id,
    COALESCE(
        asset_sender,
        tx_sender
    ),
    _INSERTED_TIMESTAMP
HAVING
    COUNT(
        DISTINCT NULLIF(
            dim_asset_id,
            'd41d8cd98f00b204e9800998ecf8427e'
        )
    ) <= 2
),
FINAL AS (
    SELECT
        t.block_timestamp AS block_timestamp,
        t.block_id AS block_id,
        t.tx_group_id AS tx_group_id,
        t.asset_sender AS purchaser,
        MAX(
            ast.asset_id
        ) AS nft_asset_id,
        SUM(
            CASE
                WHEN asset_id <> 0
                AND t.asset_sender = COALESCE(
                    snd.asset_receiver,
                    snd.receiver
                )
                AND asset_amount > 0 THEN asset_amount
                WHEN asset_id <> 0
                AND t.asset_sender = COALESCE(
                    asset_receiver,
                    snd.receiver
                )
                AND COALESCE(
                    asset_amount,
                    0
                ) = 0 THEN tx_message :aca :: DECIMAL
                ELSE 0
            END
        ) AS number_of_nfts,
        SUM(
            CASE
                WHEN asset_id = 0 THEN amount
                ELSE 0
            END
        ) AS total_sales_amount,
        SUM(
            CASE
                WHEN asset_id = 0
                AND receiver = 'RANDGVRRYGVKI3WSDG6OGTZQ7MHDLIN5RYKJBABL46K5RQVHUFV3NY5DUE' THEN amount
                ELSE 0
            END
        ) AS marketplace_fee,
        t._INSERTED_TIMESTAMP
    FROM
        group_tx t
        JOIN {{ ref('core__fact_transaction') }}
        snd
        ON t.tx_group_id = snd.tx_group_id
        AND (
            t.asset_sender = COALESCE(
                snd.asset_receiver,
                snd.receiver
            )
            OR t.asset_sender = COALESCE(
                snd.asset_sender,
                snd.tx_sender
            )
        )
        JOIN {{ ref('core__dim_asset') }}
        ast
        ON snd.dim_asset_id = ast.dim_asset_id
    WHERE
        dim_transaction_type_id IN (
            'c495d86d106bb9c67e5925d952e553f2',
            'b02a45a596bfb86fe2578bde75ff5444'
        )
    GROUP BY
        t.block_timestamp,
        t.block_id,
        t.tx_group_id,
        t.asset_sender,
        t._INSERTED_TIMESTAMP
    HAVING
        total_sales_amount <> marketplace_fee
)
SELECT
    block_timestamp,
    block_id,
    A.tx_group_id,
    purchaser,
    A.nft_asset_id,
    CASE
        WHEN decimals > 0 THEN number_of_nfts :: FLOAT / pow(
            10,
            decimals
        )
        WHEN NULLIF(
            decimals,
            0
        ) IS NULL THEN number_of_nfts :: FLOAT
    END AS number_of_nfts,
    total_sales_amount :: FLOAT / pow(
        10,
        6
    ) AS total_sales_amount,
    concat_ws(
        '-',
        block_id :: STRING,
        A.tx_group_id :: STRING,
        A.nft_asset_id :: STRING
    ) AS _unique_key,
    A._INSERTED_TIMESTAMP
FROM
    FINAL A
    JOIN {{ ref('core__dim_asset') }}
    ast
    ON A.nft_asset_id = ast.asset_id
WHERE
    number_of_nfts > 0
