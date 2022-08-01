{{ config(
    materialized = 'incremental',
    unique_key = '_unique_key',
    incremental_strategy = 'merge'
) }}

WITH nft_transfers AS(

    SELECT
        DISTINCT axfer.tx_group_id
    FROM
        {{ ref('silver_algorand__asset_transfer_transaction') }}
        axfer
        INNER JOIN {{ ref('silver_algorand__nft_asset') }}
        n
        ON n.nft_asset_id = axfer.asset_id
        LEFT JOIN (
            SELECT
                DISTINCT tx_group_id AS tx_group_id
            FROM
                flipside_dev_db.silver_algorand.payment_transaction
            WHERE
                (
                    receiver = 'XNFT36FUCFRR6CK675FW4BEBCCCOJ4HOSMGCN6J2W6ZMB34KM2ENTNQCP4'
                    OR receiver = 'RANDGVRRYGVKI3WSDG6OGTZQ7MHDLIN5RYKJBABL46K5RQVHUFV3NY5DUE'
                )
        ) AS market
        ON axfer.tx_group_id = market.tx_group_id
    WHERE
        COALESCE(
            TRY_BASE64_DECODE_STRING(
                tx_message :txn :note :: STRING
            ),
            ''
        ) != 'ab2.gallery'
        AND market.tx_group_id IS NULL

{% if is_incremental() %}
AND axfer._INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
) - INTERVAL '4 HOURS'
{% endif %}
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
        COUNT(
            DISTINCT CASE
                WHEN t.tx_type = 'axfer'
                AND nft.tx_group_id IS NOT NULL
                AND t.tx_message :txn :aamt :: NUMBER > 0 THEN sender {# ELSE 0 #}
            END
        ) AS axfer_tx_count,
        MAX(_INSERTED_TIMESTAMP) AS _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver_algorand__transactions') }}
        t
        JOIN nft_transfers nft
        ON t.tx_group_id = nft.tx_group_id
    WHERE
        (
            asset_id = 0
            OR asset_id IN (
                SELECT
                    nft_asset_id
                FROM
                    {{ ref('silver_algorand__nft_asset') }}
            )
        )

{% if is_incremental() %}
AND t._INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
) - INTERVAL '4 HOURS'
{% endif %}
GROUP BY
    t.tx_group_id
HAVING
    tx_group_id_ct >= 2
    AND pay_tx_count = 1
    AND axfer_tx_count = 1
)
SELECT
    axfer.block_timestamp,
    axfer.block_id,
    axfer.tx_group_id,
    axfer.asset_receiver AS purchaser,
    axfer.asset_id AS nft_asset_id,
    CASE
        WHEN nft.decimals > 0 THEN asset_amount :: FLOAT / pow(
            10,
            nft.decimals
        )
        WHEN NULLIF(
            nft.decimals,
            0
        ) IS NULL THEN asset_amount :: FLOAT
    END AS number_of_nfts,
    pay.amount / axfer_tx_count AS total_sales_amount,
    concat_ws(
        '-',
        axfer.block_id :: STRING,
        axfer.tx_group_id :: STRING,
        axfer.asset_id :: STRING
    ) AS _unique_key,
    A._INSERTED_TIMESTAMP
FROM
    tx_group_id_atomic A
    JOIN {{ ref('silver_algorand__asset_transfer_transaction') }}
    axfer
    ON A.tx_group_id = axfer.tx_group_id
    JOIN {{ ref('silver_algorand__nft_asset') }}
    nft
    ON axfer.asset_id = nft.nft_asset_id
    JOIN {{ ref('silver_algorand__payment_transaction') }}
    pay
    ON pay.tx_group_id = A.tx_group_id
    AND axfer.asset_receiver = pay.sender
    LEFT JOIN (
        SELECT
            DISTINCT tx_group_id
        FROM
            {{ ref('silver_algorand__application_call_transaction') }}
    ) exc
    ON A.tx_group_id = exc.tx_group_id
WHERE
    axfer.asset_amount BETWEEN 0
    AND 450
    AND pay.amount > 0
    AND exc.tx_group_id IS NULL
