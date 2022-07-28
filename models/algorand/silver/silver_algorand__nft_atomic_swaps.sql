{{ config(
    materialized = 'incremental',
    unique_key = '_unique_key',
    incremental_strategy = 'merge',
    tags = ['snowflake', 'algorand', 'asset', 'silver_algorand']
) }}

WITH nft_transfers AS(

    SELECT
        DISTINCT tx_group_id
    FROM
        {{ ref('silver_algorand__asset_transfer_transaction') }}
        axfer
        INNER JOIN {{ ref('silver_algorand__asset_nft') }}
        n
        ON n.asset_id = axfer.asset_id
),
tx_group_id_atomic AS(
    SELECT
        t.tx_group_id,
        COUNT(
            t.tx_group_id
        ) AS tx_group_id_ct,
        SUM(
            CASE
                WHEN t.tx_type = 'pay' THEN 1
                ELSE 0
            END
        ) AS pay_tx_count,
        SUM(
            CASE
                WHEN t.tx_type = 'axfer'
                AND nft.tx_group_id IS NOT NULL
                AND t.tx_message :txn :aamt :: NUMBER > 0 THEN 1
                ELSE 0
            END
        ) AS axfer_tx_count
    FROM
        {{ ref('silver_algorand__transactions') }}
        t
        LEFT JOIN nft_transfers nft
        ON t.tx_group_id = nft.tx_group_id
    GROUP BY
        t.tx_group_id
    HAVING
        tx_group_id_ct > 2
        AND pay_tx_count = 1
        AND axfer_tx_count = 1
)
SELECT
    axfer.block_timestamp,
    axfer.block_id,
    axfer.tx_group_id,
    axfer.asset_receiver AS purchaser,
    axfer.asset_id AS nft_asset_id,
    axfer.asset_amount AS number_of_nfts,
    pay.amount AS total_sales_amount,
    concat_ws(
        '-',
        axfer.block_id :: STRING,
        axfer.tx_group_id :: STRING,
        axfer.asset_id :: STRING
    ) AS _unique_key
FROM
    tx_group_id_atomic A
    LEFT JOIN {{ ref('silver_algorand__asset_transfer_transaction') }}
    axfer
    ON A.tx_group_id = axfer.tx_group_id
    LEFT JOIN {{ ref('silver_algorand__payment_transaction') }}
    pay
    ON pay.tx_group_id = A.tx_group_id
WHERE
    axfer.asset_receiver = pay.sender
    AND axfer.asset_amount > 0
    AND pay.amount > 0
