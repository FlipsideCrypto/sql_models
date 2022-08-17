{{ config(
    materialized = 'incremental',
    unique_key = '_unique_key',
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH atran AS (

    SELECT
        A.tx_group_id,
        block_id,
        asset_receiver,
        A.asset_id,
        decimals,
        tx_message,
        asset_amount
    FROM
        {{ ref('silver__transaction') }} A
        JOIN {{ ref('silver__asset') }}
        nft
        ON A.asset_id = nft.asset_id
        LEFT JOIN (
            SELECT
                DISTINCT tx_group_id AS tx_group_id
            FROM
                {{ ref('silver__transaction') }} A
            WHERE
                tx_type = 'pay'
                AND (
                    receiver = 'XNFT36FUCFRR6CK675FW4BEBCCCOJ4HOSMGCN6J2W6ZMB34KM2ENTNQCP4'
                    OR receiver = 'RANDGVRRYGVKI3WSDG6OGTZQ7MHDLIN5RYKJBABL46K5RQVHUFV3NY5DUE'
                )

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
) AS market
ON A.tx_group_id = market.tx_group_id
WHERE
    tx_type = 'axfer'
    AND is_nft = TRUE
    AND market.tx_group_id IS NULL

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
pt AS (
    SELECT
        tx_group_id,
        amount,
        sender
    FROM
        {{ ref('silver__transaction') }}
    WHERE
        tx_type = 'pay'

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
nft_transfers AS(
    SELECT
        DISTINCT tx_group_id
    FROM
        atran axfer
    WHERE
        COALESCE(
            TRY_BASE64_DECODE_STRING(
                tx_message :txn :note :: STRING
            ),
            ''
        ) != 'ab2.gallery'
),
tx_group_id_atomic AS(
    SELECT
        t.tx_group_id,
        t.block_id,
        COUNT(
            t.tx_group_id
        ) AS tx_group_id_ct,
        SUM(
            CASE
                WHEN tx_type = 'pay' THEN 1
                ELSE 0
            END
        ) AS pay_tx_count,
        COUNT(
            DISTINCT CASE
                WHEN tx_type = 'axfer'
                AND nft.tx_group_id IS NOT NULL
                AND t.tx_message :txn :aamt :: NUMBER > 0 THEN sender {# ELSE 0 #}
            END
        ) AS axfer_tx_count,
        MAX(
            t._INSERTED_TIMESTAMP
        ) AS _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__transaction') }}
        t
        JOIN nft_transfers nft
        ON t.tx_group_id = nft.tx_group_id
    WHERE
        (
            asset_id = 0
            OR asset_id IN (
                SELECT
                    asset_id
                FROM
                    {{ ref('silver__asset') }}
                WHERE
                    is_nft = TRUE
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
    t.block_id,
    t.tx_group_id
HAVING
    tx_group_id_ct >= 2
    AND pay_tx_count = 1
    AND axfer_tx_count = 1
)
SELECT
    b.block_timestamp,
    axfer.block_id,
    axfer.tx_group_id,
    axfer.asset_receiver AS purchaser,
    axfer.asset_id AS nft_asset_id,
    CASE
        WHEN decimals > 0 THEN asset_amount :: FLOAT / pow(
            10,
            decimals
        )
        WHEN NULLIF(
            decimals,
            0
        ) IS NULL THEN asset_amount :: FLOAT
    END AS number_of_nfts,
    pay.amount :: FLOAT / pow(
        10,
        6
    ) / axfer_tx_count AS total_sales_amount,
    concat_ws(
        '-',
        axfer.block_id :: STRING,
        axfer.tx_group_id :: STRING,
        axfer.asset_id :: STRING
    ) AS _unique_key,
    A._INSERTED_TIMESTAMP
FROM
    tx_group_id_atomic A
    JOIN {{ ref('silver__block') }}
    b
    ON A.block_id = b.block_id
    JOIN atran axfer
    ON A.tx_group_id = axfer.tx_group_id
    JOIN pt pay
    ON pay.tx_group_id = A.tx_group_id
    AND axfer.asset_receiver = pay.sender
    LEFT JOIN (
        SELECT
            DISTINCT tx_group_id
        FROM
            {{ ref('silver__transaction') }}
        WHERE
            tx_type = 'appl'
    ) exc
    ON A.tx_group_id = exc.tx_group_id
WHERE
    axfer.asset_amount BETWEEN 0
    AND 450
    AND pay.amount > 0
    AND exc.tx_group_id IS NULL
