{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['block_hour']
) }}

WITH lps AS (

    SELECT
        address
    FROM
        {{ ref('silver__pool_addresses') }} C
    WHERE
        C.label = 'tinyman'
        AND label_subtype = 'pool'
        AND (
            C.address_name LIKE '%-ALGO %'
            OR C.address_name LIKE '%ALGO-%'
        )
        AND C.address_name NOT ILIKE '%algo fam%'
        AND C.address_name NOT ILIKE '%smart algo%'
),
hourly_prices_with_gaps AS (
    SELECT
        HOUR,
        AVG(price) price
    FROM
        (
            SELECT
                p.symbol,
                DATE_TRUNC(
                    'hour',
                    recorded_at
                ) AS HOUR,
                price
            FROM
                {{ source(
                    'shared',
                    'prices_v2'
                ) }}
                p
            WHERE
                asset_id IN ('algorand', '4030')

{% if is_incremental() %}
AND recorded_at :: DATE >= CURRENT_DATE - 3
{% else %}
    AND recorded_at >= '2022-01-01'
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY DATE_TRUNC('hour', recorded_at), provider
ORDER BY
    recorded_at DESC)) = 1
) x
GROUP BY
    HOUR
),
hourly_prices AS (
    SELECT
        DATE AS HOUR,
        LAST_VALUE(
            price ignore nulls
        ) over(
            ORDER BY
                DATE ASC rows unbounded preceding
        ) AS price
    FROM
        (
            SELECT
                DISTINCT DATE
            FROM
                {{ ref('silver__hourly_pool_balances') }}

{% if is_incremental() %}
WHERE
    DATE :: DATE >= CURRENT_DATE - 3
{% endif %}
) A
LEFT JOIN hourly_prices_with_gaps b
ON A.date = b.hour
),
balances AS (
    SELECT
        A.address,
        A.date,
        OBJECT_AGG(
            CASE
                WHEN A.asset_id = 0 THEN 'algo'
                ELSE 'other'
            END,
            balance :: variant
        ) AS j,
        OBJECT_AGG(
            CASE
                WHEN A.asset_id = 0 THEN 'algo'
                ELSE 'other'
            END,
            A.asset_id :: variant
        ) AS j_2,
        OBJECT_AGG(
            CASE
                WHEN A.asset_id = 0 THEN 'algo'
                ELSE 'other'
            END,
            d.asset_name :: variant
        ) AS j_3,
        j :algo AS algo_bal,
        j :other AS other_bal,
        j_2 :other AS other_asset_id,
        j_3 :other :: STRING AS other_asset_name
    FROM
        {{ ref('silver__hourly_pool_balances') }} A
        JOIN lps b
        ON A.address = b.address
        LEFT JOIN {{ ref('silver__asset') }}
        d
        ON A.asset_id = d.asset_ID
    WHERE
        (
            (LOWER(asset_name) NOT LIKE '%pool%'
            AND LOWER(asset_name) NOT LIKE '%lp%')
            OR A.asset_Id = 0)
            AND COALESCE(
                asset_units,
                ''
            ) <> 'SILO'

{% if is_incremental() %}
AND DATE :: DATE >= CURRENT_DATE - 3
{% endif %}
GROUP BY
    A.address,
    A.date
)
SELECT
    A.date AS block_hour,
    other_asset_ID AS asset_id,
    other_asset_name AS asset_name,
    CASE
        WHEN other_bal = 0 THEN 0
        ELSE (
            algo_bal * price
        ) / other_bal
    END AS price_usd,
    algo_bal AS algo_balance,
    other_bal AS non_algo_balance,
    e.address_name pool_name,
    A.address pool_address,
    concat_ws(
        '-',
        block_hour,
        asset_id
    ) AS _unique_key,
    price AS _algo_price
FROM
    balances A
    JOIN hourly_prices C
    ON A.date = C.hour
    LEFT JOIN {{ ref('silver__pool_addresses') }}
    e
    ON A.address = e.address
WHERE
    other_asset_ID IS NOT NULL
UNION ALL
SELECT
    HOUR AS block_hour,
    0 AS asset_id,
    'ALGO' AS asset_name,
    price AS price_usd,
    NULL algo_balance,
    NULL non_algo_balance,
    NULL pool_name,
    NULL pool_address,
    concat_ws(
        '-',
        HOUR,
        0
    ) unique_key,
    price AS _algo_price
FROM
    hourly_prices
