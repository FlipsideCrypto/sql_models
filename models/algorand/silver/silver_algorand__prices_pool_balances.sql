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
        {{ ref('silver_algorand__pool_addresses') }} C
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
hourly_prices AS (
    SELECT
        symbol,
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
AND recorded_at >= CURRENT_DATE - 3
{% else %}
    AND recorded_at >= '2022-01-01'
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY DATE_TRUNC('hour', recorded_at), provider
ORDER BY
    recorded_at DESC)) = 1
) x
GROUP BY
    symbol,
    HOUR
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
        j :algo AS algo_bal,
        j :other AS other_bal,
        j_2 :other AS other_asset_ID
    FROM
        {{ ref('silver_algorand__hourly_pool_balances') }} A
        JOIN lps b
        ON A.address = b.address
        LEFT JOIN {{ ref('silver_algorand__asset') }}
        d
        ON A.asset_id = d.asset_ID
    WHERE
        (
            (LOWER(asset_name) NOT LIKE '%pool%'
            AND LOWER(asset_name) NOT LIKE '%lp%')
            OR A.asset_Id = 0)
            GROUP BY
                A.address,
                A.date
        )
    SELECT
        A.date AS block_hour,
        other_asset_ID AS asset_id,
        (
            algo_bal * price
        ) / NULLIF(
            other_bal,
            0
        ) AS price_usd,
        algo_bal AS algo_balance,
        other_bal AS non_algo_balance,
        e.address_name pool_name,
        A.address pool_address,
        concat_ws(
            '-',
            block_hour,
            asset_id
        ) AS _unique_key
    FROM
        balances A
        JOIN hourly_prices C
        ON A.date = C.hour
        LEFT JOIN {{ ref('silver_algorand__pool_addresses') }}
        e
        ON A.address = e.address
    WHERE
        other_asset_ID IS NOT NULL
