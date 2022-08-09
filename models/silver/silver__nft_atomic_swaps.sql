{{ config(
    materialized = 'incremental',
    unique_key = '_unique_key',
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH atran AS (

    SELECT
        A.tx_group_id,
        block_timestamp,
        block_id,
        asset_receiver,
        asset_id,
        decimals,
        tx_message,
        asset_amount
    FROM
        {{ ref('core__fact_transaction') }} A
        JOIN {{ ref('core__dim_asset') }}
        nft
        ON A.dim_asset_id = nft.dim_asset_id
        LEFT JOIN (
            SELECT
                DISTINCT tx_group_id AS tx_group_id
            FROM
                {{ ref('core__fact_transaction') }} A
            WHERE
                dim_transaction_type_id = 'b02a45a596bfb86fe2578bde75ff5444'
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
    dim_transaction_type_id = 'c495d86d106bb9c67e5925d952e553f2'
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
        tx_sender
    FROM
        {{ ref('core__fact_transaction') }}
    WHERE
        dim_transaction_type_id = 'b02a45a596bfb86fe2578bde75ff5444'

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
        COUNT(
            t.tx_group_id
        ) AS tx_group_id_ct,
        SUM(
            CASE
                WHEN dim_transaction_type_id = 'b02a45a596bfb86fe2578bde75ff5444' THEN 1
                ELSE 0
            END
        ) AS pay_tx_count,
        COUNT(
            DISTINCT CASE
                WHEN dim_transaction_type_id = 'c495d86d106bb9c67e5925d952e553f2'
                AND nft.tx_group_id IS NOT NULL
                AND t.tx_message :txn :aamt :: NUMBER > 0 THEN tx_sender {# ELSE 0 #}
            END
        ) AS axfer_tx_count,
        MAX(
            t._INSERTED_TIMESTAMP
        ) AS _INSERTED_TIMESTAMP
    FROM
        {{ ref('core__fact_transaction') }}
        t
        JOIN nft_transfers nft
        ON t.tx_group_id = nft.tx_group_id
        JOIN {{ ref('core__dim_asset') }}
        ast
        ON t.dim_asset_id = ast.dim_asset_id
    WHERE
        (
            asset_id = 0
            OR asset_id IN (
                SELECT
                    asset_id
                FROM
                    {{ ref('core__dim_asset') }}
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
    JOIN atran axfer
    ON A.tx_group_id = axfer.tx_group_id
    JOIN pt pay
    ON pay.tx_group_id = A.tx_group_id
    AND axfer.asset_receiver = pay.tx_sender
    LEFT JOIN (
        SELECT
            DISTINCT tx_group_id
        FROM
            {{ ref('core__fact_transaction') }}
        WHERE
            dim_transaction_type_id = '63469c3c4f19f07c737127a117296de4'
    ) exc
    ON A.tx_group_id = exc.tx_group_id
WHERE
    axfer.asset_amount BETWEEN 0
    AND 450
    AND pay.amount > 0
    AND exc.tx_group_id IS NULL
