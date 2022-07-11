{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_hour, asset_id)",
    incremental_strategy = 'merge',
    cluster_by = ['block_hour']
) }}

WITH lps AS (

    SELECT
        address,
        DATE
    FROM
        {{ ref('silver_algorand__hourly_pool_balances') }}
    WHERE
        asset_id = 0
        AND balance > 10000
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
idk AS (
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
        AND A.date = b.date
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
        A.address,
        A.date AS block_hour,
        other_asset_ID AS asset_id,
        d.asset_name,
        algo_bal AS algo_balance,
        other_bal AS non_algo_balance,
        (
            algo_bal * price
        ) / other_bal AS price_usd,
        e.address_name,
        e.label
    FROM
        idk A
        JOIN hourly_prices C
        ON A.date = C.hour
        LEFT JOIN {{ ref('silver_algorand__asset') }}
        d
        ON A.other_asset_ID = d.asset_ID
        LEFT JOIN {{ ref('silver_algorand__pool_addresses') }}
        e
        ON A.address = e.address
