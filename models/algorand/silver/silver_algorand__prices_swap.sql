{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_hour, asset_id)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_hour'],
    tags = ['snowflake', 'algorand', 'transactions', 'algorand_swaps', 'algorand_prices']
) }}

WITH swaps AS (

    SELECT
        block_hour,
        swap_from_asset_id,
        swap_from_amount,
        swap_to_asset_id,
        swap_to_amount,
        dex
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
                swap_to_amount,
                'algofi' AS dex
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
                swap_to_amount,
                'pactfi' AS dex
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
                swap_to_amount,
                'tinyman' AS dex
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
                swap_to_amount,
                'wagmiswap' AS dex
            FROM
                {{ ref('silver_algorand__swaps_wagmiswap_dex') }}
        ) x
    WHERE
        swap_from_amount > 0
        AND swap_to_amount > 0

{% if is_incremental() %}
AND block_hour >=(
    SELECT
        DATEADD('day', -1, MAX(block_hour :: DATE))
    FROM
        {{ this }}
)
{% endif %}

qualify(RANK() over(
ORDER BY
    block_hour DESC)) <> 1
),
swap_range AS (
    SELECT
        MIN(
            block_hour
        ) min_date,
        MAX(
            block_hour
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
        END AS to_usd,
        dex
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
        END AS to_usd,
        dex
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
        from_usd price,
        dex,
        from_amt amt
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
        to_usd,
        dex,
        to_amt amt
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
        to_usd,
        dex,
        to_amt amt
    FROM
        algo
    WHERE
        from_asset_id = '0'
    UNION ALL
    SELECT
        block_hour,
        from_asset_id,
        from_asset_name,
        from_usd,
        dex,
        from_amt amt
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
        ) stddev_price,
        dex,
        amt
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
        ) avg_price,
        dex,
        amt
    FROM
        combo_2
),
final_dex AS (
    SELECT
        block_hour,
        block_hour :: DATE AS block_date,
        asset_id,
        asset_name,
        dex,
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
        COUNT(1) swaps_in_hour,
        SUM(amt) total_amt
    FROM
        combo_3
    GROUP BY
        1,
        2,
        3,
        4,
        5
),
weights AS (
    SELECT
        dex,
        asset_id,
        block_date,
        total_amt / SUM(total_amt) over(
            PARTITION BY asset_id,
            block_date
        ) vol_weight,
        swaps_in_day / SUM(swaps_in_day) over(
            PARTITION BY asset_id,
            block_date
        ) swaps_weight
    FROM
        (
            SELECT
                dex,
                asset_id,
                block_hour :: DATE block_date,
                SUM(total_amt) total_amt,
                SUM(swaps_in_hour) swaps_in_day
            FROM
                final_dex
            GROUP BY
                1,
                2,
                3
        ) z
),
FINAL AS (
    SELECT
        block_hour,
        A.asset_id,
        asset_name,
        MIN(min_price_usd_hour) AS min_price_usd_hour,
        MAX(max_price_usd_hour) AS max_price_usd_hour,
        MAX(max_price_usd_hour) - MIN(min_price_usd_hour) AS volatility_measure,
        SUM(swaps_in_hour) AS swaps_in_hour,
        SUM(total_amt) AS volume_in_hour,
        SUM(
            avg_price_usd_hour_excludes * COALESCE(
                vol_weight,
                1
            )
        ) price
    FROM
        final_dex A
        LEFT JOIN weights b
        ON A.asset_ID = b.asset_id
        AND A.dex = b.dex
        AND A.block_date = b.block_date
    GROUP BY
        1,
        2,
        3
)

{% if is_incremental() %},
not_in_final AS (
    SELECT
        DATEADD(
            'HOUR',
            1,
            block_hour
        ) block_hour,
        asset_id,
        asset_name,
        0 AS min_price_usd_hour,
        0 AS max_price_usd_hour,
        0 AS volatility_measure,
        0 AS swaps_in_hour,
        0 AS volume_in_hour,
        price_usd
    FROM
        {{ this }}
    WHERE
        asset_id NOT IN(
            SELECT
                DISTINCT asset_id
            FROM
                FINAL
        )
)
{% endif %},
fill_in_the_blanks_temp AS (
    SELECT
        A.hour AS block_hour,
        b.asset_id,
        b.asset_name,
        C.price,
        C.min_price_usd_hour,
        C.max_price_usd_hour,
        C.volatility_measure,
        C.swaps_in_hour,
        C.volume_in_hour
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
    )
    AND HOUR <= (
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

{% if is_incremental() %}
UNION
SELECT
    DISTINCT asset_id,
    asset_name
FROM
    {{ this }}
{% endif %}
) b
LEFT JOIN (
    SELECT
        *
    FROM
        FINAL

{% if is_incremental() %}
UNION ALL
SELECT
    *
FROM
    not_in_final
{% endif %}
) C
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
    ) AS price_usd,
    min_price_usd_hour,
    max_price_usd_hour,
    volatility_measure,
    swaps_in_hour,
    volume_in_hour AS volume_usd_in_hour
FROM
    fill_in_the_blanks_temp qualify(LAST_VALUE(price ignore nulls) over(PARTITION BY asset_id
ORDER BY
    block_hour ASC rows unbounded preceding)) IS NOT NULL
