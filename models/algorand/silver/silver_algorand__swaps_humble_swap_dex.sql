{{ config(
    materialized = 'incremental',
    unique_key = '_unique_key',
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH config_tx AS (

    SELECT
        tx_id,
        sender
    FROM
        {{ ref('silver_algorand__application_call_transaction') }}
    WHERE
        app_id = 771884869
        AND block_timestamp :: DATE >= '2022-06-09'
),
app_ids AS (
    SELECT
        A.app_id,
        A.tx_group_id,
        b.sender AS pool_address,
        block_ID
    FROM
        {{ ref('silver_algorand__application_call_transaction') }} A
        JOIN config_tx b
        ON A.tx_ID = b.tx_ID
    WHERE
        app_id <> 771884869
        AND block_timestamp :: DATE >= '2022-06-09'
),
hs_tx_group_ids AS (
    SELECT
        A.block_timestamp,
        A.block_id,
        MIN(
            A.intra
        ) AS intra,
        A.tx_group_id,
        A.app_id,
        b.pool_address,
        A.sender AS swapper,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver_algorand__application_call_transaction') }} A
        JOIN app_ids b
        ON b.app_id = A.app_id
    WHERE
        A.block_timestamp :: DATE >= '2022-06-09'

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
    A.block_timestamp,
    A.block_id,
    A.tx_group_id,
    A.app_id,
    b.pool_address,
    A.sender,
    A._INSERTED_TIMESTAMP
),
hs_transfers AS(
    SELECT
        A.tx_group_id,
        A.asset_sender,
        A.asset_id,
        A.amount,
        A.receiver,
        COALESCE(
            c1.pool_address,
            c2.pool_address
        ) AS pool_address,
        A.intra
    FROM
        {{ ref('silver_algorand__transfers') }} A
        LEFT JOIN app_ids c1
        ON A.asset_sender = c1.pool_address
        LEFT JOIN app_ids c2
        ON A.receiver = c2.pool_address
        LEFT JOIN app_ids c3
        ON A.tx_group_id = c3.tx_group_id
    WHERE
        A.block_timestamp :: DATE >= '2022-06-09'
        AND A.tx_group_id IN (
            SELECT
                tx_group_id
            FROM
                hs_tx_group_ids b
        )
        AND amount > 0
        AND (
            c1.pool_address IS NOT NULL
            OR c2.pool_address IS NOT NULL
        )
        AND c3.tx_group_id IS NULL

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
pool_counts AS (
    SELECT
        tx_group_id,
        pool_address,
        MOD (COUNT(1), 2) xfer_mod
    FROM
        hs_transfers
    GROUP BY
        tx_group_id,
        pool_address
    HAVING
        xfer_mod = 0),
        net AS (
            SELECT
                A.block_timestamp,
                A.block_id,
                A.intra,
                A.tx_group_id,
                A.swapper,
                A.app_id,
                b.pool_address,
                A._INSERTED_TIMESTAMP,
                b.asset_id,
                ABS(
                    SUM(
                        CASE
                            WHEN asset_sender = swapper THEN - amount
                            ELSE amount
                        END
                    )
                ) AS amount,
                ROW_NUMBER() over(
                    PARTITION BY A.tx_group_id,
                    b.pool_address
                    ORDER BY
                        MIN(
                            b.intra
                        )
                ) orderby
            FROM
                hs_tx_group_ids A
                JOIN hs_transfers b
                ON A.tx_group_id = b.tx_group_id
                AND A.pool_address = b.pool_address
                JOIN pool_counts pc
                ON b.tx_group_id = pc.tx_group_id
                AND b.pool_address = pc.pool_address
            GROUP BY
                A.block_timestamp,
                A.block_id,
                A.intra,
                A.tx_group_id,
                A.swapper,
                A.app_id,
                b.pool_address,
                A._INSERTED_TIMESTAMP,
                b.asset_id
        ),
        swaps AS (
            SELECT
                A.block_timestamp,
                A.block_id,
                A.intra,
                A.tx_group_id,
                A.swapper,
                A.app_id,
                A.pool_address,
                A._INSERTED_TIMESTAMP,
                OBJECT_AGG(
                    CASE
                        WHEN orderby = 1 THEN 'from_asset'
                        ELSE 'to_asset'
                    END :: STRING,
                    asset_id :: variant
                ) AS j_asset,
                OBJECT_AGG(
                    CASE
                        WHEN orderby = 1 THEN 'from_amount'
                        ELSE 'to_amount'
                    END :: STRING,
                    amount :: variant
                ) AS j_amount,
                j_asset :from_asset AS swap_from_asset_id,
                j_asset :to_asset AS swap_to_asset_id,
                j_amount :from_amount :: FLOAT AS swap_from_amount,
                j_amount :to_amount :: FLOAT AS swap_to_amount
            FROM
                net A
            GROUP BY
                A.block_timestamp,
                A.block_id,
                A.intra,
                A.tx_group_id,
                A.swapper,
                A.app_id,
                A.pool_address,
                A._INSERTED_TIMESTAMP
        )
    SELECT
        block_timestamp,
        block_id,
        tx_group_id,
        intra,
        app_id,
        swapper,
        swap_from_asset_id,
        swap_from_amount,
        pool_address,
        swap_to_asset_id,
        swap_to_amount,
        concat_ws(
            '-',
            block_id :: STRING,
            intra :: STRING
        ) AS _unique_key,
        _INSERTED_TIMESTAMP
    FROM
        swaps
