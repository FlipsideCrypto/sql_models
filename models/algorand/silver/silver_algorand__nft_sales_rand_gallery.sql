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
        asset_sender,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver_algorand__transfers') }}
    WHERE
        receiver = 'RANDGVRRYGVKI3WSDG6OGTZQ7MHDLIN5RYKJBABL46K5RQVHUFV3NY5DUE' {# AND asset_sender != 'RANDGVRRYGVKI3WSDG6OGTZQ7MHDLIN5RYKJBABL46K5RQVHUFV3NY5DUE' #}
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
    asset_sender,
    _INSERTED_TIMESTAMP
HAVING
    COUNT(
        DISTINCT asset_id
    ) <= 2
),
FINAL AS (
    SELECT
        t.block_timestamp AS block_timestamp,
        t.block_id AS block_id,
        t.tx_group_id AS tx_group_id,
        t.asset_sender AS purchaser,
        MAX(
            send.asset_id
        ) AS nft_asset_id,
        SUM(
            CASE
                WHEN asset_id <> 0
                AND t.asset_sender = receiver
                AND amount > 0 THEN amount
                WHEN asset_id <> 0
                AND t.asset_sender = receiver
                AND amount = 0 THEN tx_message :aca :: DECIMAL
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
        JOIN {{ ref('silver_algorand__transfers') }}
        send
        ON t.tx_group_id = send.tx_group_id
        AND (
            t.asset_sender = send.receiver
            OR t.asset_sender = send.asset_sender
        )
    WHERE
        send.tx_group_id IS NOT NULL
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
    tx_group_id,
    purchaser,
    nft_asset_id,
    number_of_nfts,
    total_sales_amount,
    concat_ws(
        '-',
        block_id :: STRING,
        tx_group_id :: STRING,
        nft_asset_id :: STRING
    ) AS _unique_key,
    _INSERTED_TIMESTAMP
FROM
    FINAL
WHERE
    number_of_nfts > 0
