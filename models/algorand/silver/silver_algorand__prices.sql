{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_hour, asset_id)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_hour'],
    tags = ['snowflake', 'algorand', 'transactions', 'algorand__swaps', 'algorand_prices']
) }}

WITH swaps AS (

    SELECT
        block_hour,
        block_hour :: datetime block_hour_datetime,
        swap_from_asset_id,
        swap_from_amount,
        swap_to_asset_id,
        swap_to_amount
    FROM
        (
            SELECT
                DATE_TRUNC(
                    'HOUR',
                    block_timestamp
                ) AS block_hour,
                swap_from_asset_id,
                swap_from_amount,
                swap_to_asset_id,
                swap_to_amount
            FROM
                {{ ref('silver_algorand__swaps_algofi_dex') }}
            UNION ALL
            SELECT
                DATE_TRUNC(
                    'HOUR',
                    block_timestamp
                ) AS block_hour,
                swap_from_asset_id,
                swap_from_amount,
                swap_to_asset_id,
                swap_to_amount
            FROM
                {{ ref('silver_algorand__swaps_pactfi_dex') }}
            UNION ALL
            SELECT
                DATE_TRUNC(
                    'HOUR',
                    block_timestamp
                ) AS block_hour,
                swap_from_asset_id,
                swap_from_amount,
                swap_to_asset_id,
                swap_to_amount
            FROM
                {{ ref('silver_algorand__swaps_tinyman_dex') }}
            UNION ALL
            SELECT
                DATE_TRUNC(
                    'HOUR',
                    block_timestamp
                ) AS block_hour,
                swap_from_asset_id,
                swap_from_amount,
                swap_to_asset_id,
                swap_to_amount
            FROM
                {{ ref('silver_algorand__swaps_wagmiswap_dex') }}
        ) x
    WHERE
        swap_from_amount > 0
        AND swap_to_amount > 0

{% if is_incremental() %}
AND block_hour >(
    SELECT
        MAX(block_hour)
    FROM
        {{ this }}
)
{% endif %}
),
swap_range AS (
    SELECT
        MIN(
            block_hour_datetime
        ) min_date,
        MAX(
            block_hour_datetime
        ) max_date
    FROM
        swaps
),
usd AS (
    SELECT
        block_hour,
        swap_from_asset_id AS from_asset_id,
        COALESCE(
            f.asset_name,
            'ALGO'
        ) AS from_asset_name,
        swap_from_amount AS from_amt,CASE
            WHEN swap_from_asset_id IN (
                '31566704',
                '312769'
            ) THEN swap_from_amount
            ELSE swap_to_amount / NULLIF(
                swap_from_amount,
                0
            )
        END AS from_usd,
        swap_to_asset_id AS to_asset_id,
        COALESCE(
            t.asset_name,
            'ALGO'
        ) AS to_asset_name,
        swap_to_amount AS to_amt,CASE
            WHEN swap_to_asset_id IN (
                '31566704',
                '312769'
            ) THEN swap_to_amount
            ELSE swap_from_amount / NULLIF(
                swap_to_amount,
                0
            )
        END AS to_usd
    FROM
        swaps A
        LEFT JOIN {{ ref('silver_algorand__asset') }}
        f
        ON A.swap_from_asset_id = f.asset_id
        LEFT JOIN {{ ref('silver_algorand__asset') }}
        t
        ON A.swap_to_asset_id = t.asset_id
),
usd_2 AS (
    SELECT
        block_hour,
        from_asset_id asset_id,
        from_asset_name asset_name,
        from_usd price
    FROM
        usd
    WHERE
        to_asset_id IN (
            '31566704',
            '312769'
        )
    UNION ALL
    SELECT
        block_hour,
        to_asset_id,
        to_asset_name,
        to_usd
    FROM
        usd
    WHERE
        from_asset_id IN (
            '31566704',
            '312769'
        )
),
usd_3 AS (
    SELECT
        block_hour,
        asset_id,
        asset_name,
        price,
        STDDEV(
            price
        ) over (
            PARTITION BY asset_id,
            block_hour
        ) stddev_price
    FROM
        usd_2
    WHERE
        asset_ID = '0'
),
usd_4 AS (
    SELECT
        block_hour,
        asset_id,
        asset_name,
        price,
        stddev_price,
        CASE
            WHEN ABS(price - AVG(price) over(PARTITION BY asset_ID, block_hour)) > stddev_price * 2 THEN TRUE
            ELSE FALSE
        END exclude_from_pricing,
        AVG(price) over(
            PARTITION BY asset_ID,
            block_hour
        ) avg_price
    FROM
        usd_3
),
algo_price_hour AS (
    SELECT
        block_hour,
        AVG(
            CASE
                WHEN exclude_from_pricing = FALSE THEN price
            END
        ) price
    FROM
        usd_4
    GROUP BY
        1
),
algo AS (
    SELECT
        A.block_hour,
        swap_from_asset_id AS from_asset_id,
        COALESCE(
            f.asset_name,
            'ALGO'
        ) AS from_asset_name,
        swap_from_amount AS from_amt,CASE
            WHEN swap_to_asset_id = '0' THEN (
                swap_to_amount * prices.price
            ) / NULLIF(
                swap_from_amount,
                0
            )
        END AS from_usd,
        swap_to_asset_id AS to_asset_id,
        COALESCE(
            t.asset_name,
            'ALGO'
        ) AS to_asset_name,
        swap_to_amount AS to_amt,CASE
            WHEN swap_from_asset_id = '0' THEN (
                swap_from_amount * prices.price
            ) / NULLIF(
                swap_to_amount,
                0
            )
        END AS to_usd
    FROM
        swaps A
        LEFT JOIN {{ ref('silver_algorand__asset') }}
        f
        ON A.swap_from_asset_id = f.asset_id
        LEFT JOIN {{ ref('silver_algorand__asset') }}
        t
        ON A.swap_to_asset_id = t.asset_id
        LEFT JOIN algo_price_hour prices
        ON A.block_hour = prices.block_hour
    WHERE
        (
            A.swap_from_asset_id = '0'
            OR A.swap_to_asset_id = '0'
        )
        AND NOT (
            A.swap_from_asset_id = '0'
            AND A.swap_to_asset_id = '0'
        )
),
combo_1 AS (
    SELECT
        block_hour,
        from_asset_id asset_id,
        from_asset_name asset_name,
        from_usd price
    FROM
        usd
    WHERE
        to_asset_id IN (
            '31566704',
            '312769'
        )
    UNION ALL
    SELECT
        block_hour,
        to_asset_id,
        to_asset_name,
        to_usd
    FROM
        usd
    WHERE
        from_asset_id IN (
            '31566704',
            '312769'
        )
    UNION ALL
    SELECT
        block_hour,
        to_asset_id,
        to_asset_name,
        to_usd
    FROM
        algo
    WHERE
        from_asset_id = '0'
    UNION ALL
    SELECT
        block_hour,
        from_asset_id,
        from_asset_name,
        from_usd
    FROM
        algo
    WHERE
        to_asset_id = '0'
),
combo_2 AS (
    SELECT
        block_hour,
        asset_id,
        asset_name,
        price,
        STDDEV(
            price
        ) over (
            PARTITION BY asset_id,
            block_hour
        ) stddev_price
    FROM
        combo_1
),
combo_3 AS (
    SELECT
        block_hour,
        asset_id,
        asset_name,
        price,
        stddev_price,
        CASE
            WHEN ABS(price - AVG(price) over(PARTITION BY asset_ID, block_hour)) > stddev_price * 2 THEN TRUE
            ELSE FALSE
        END exclude_from_pricing,
        AVG(price) over(
            PARTITION BY asset_ID,
            block_hour
        ) avg_price
    FROM
        combo_2
),
FINAL AS (
    SELECT
        block_hour,
        asset_id,
        asset_name,
        AVG(
            price
        ) avg_price_usd_hour_all,
        AVG(
            CASE
                WHEN exclude_from_pricing = FALSE THEN price
            END
        ) avg_price_usd_hour_excludes,
        MIN(
            price
        ) min_price_usd_hour,
        MAX(
            price
        ) max_price_usd_hour,
        MAX(
            price
        ) - MIN(
            price
        ) AS volatility_measure,
        AVG(
            price
        ) - AVG(
            CASE
                WHEN exclude_from_pricing = FALSE THEN price
            END
        ) AS avg_adjustment_amount,
        COUNT(1) swaps_in_hour
    FROM
        combo_3
    GROUP BY
        1,
        2,
        3
),
fill_in_the_blanks_temp AS (
    SELECT
        A.hour AS block_hour,
        b.asset_id,
        b.asset_name,
        C.avg_price_usd_hour_excludes AS price
    FROM
        (
            SELECT
                HOUR
            FROM
                {{ source(
                    'shared',
                    'hours'
                ) }} A

{% if is_incremental() %}
WHERE
    HOUR > (
        SELECT
            MAX(block_hour)
        FROM
            {{ this }}
    ) <= (
        SELECT
            max_date
        FROM
            swap_range
    )
{% else %}
    JOIN swap_range b
    ON A.hour BETWEEN b.min_date
    AND max_date
{% endif %}
) A
CROSS JOIN (
    SELECT
        DISTINCT asset_id,
        asset_name
    FROM
        FINAL
) b
LEFT JOIN FINAL C
ON A.hour = C.block_hour
AND b.asset_ID = C.asset_ID
)
SELECT
    block_hour,
    asset_id,
    asset_name,
    LAST_VALUE(
        price ignore nulls
    ) over(
        PARTITION BY asset_id
        ORDER BY
            block_hour ASC rows unbounded preceding
    ) AS price
FROM
    fill_in_the_blanks_temp qualify(LAST_VALUE(price ignore nulls) over(PARTITION BY asset_id
ORDER BY
    block_hour ASC rows unbounded preceding)) IS NOT NULL
