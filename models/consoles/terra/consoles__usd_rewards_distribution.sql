-- Velocity: 115cafb9-4e82-4967-9c43-906b706760e4
{{ config(
    materialized = 'view',
    unique_key = "CONCAT_WS('-', day, type)",
    tags = ['snowflake', 'terra', 'console']
) }}

WITH total_supply AS (

    SELECT
        DATE AS DAY,
        SUM(balance) AS daily_total_supply
    FROM
        {{ ref('terra__daily_balances') }}
    WHERE
        currency = 'LUNA'
        AND address != 'terra1fl48vsnmsdzcv85q5d2q4z5ajdha8yu3nln0mh'
    GROUP BY
        DAY
),
staked AS (
    SELECT
        DATE AS DAY,
        SUM(balance) AS total_stake
    FROM
        {{ ref('terra__daily_balances') }}
    WHERE
        currency = 'LUNA'
        AND address != 'terra1fl48vsnmsdzcv85q5d2q4z5ajdha8yu3nln0mh'
        AND balance_type = 'staked'
    GROUP BY
        DAY
)
SELECT
    'total_supply' AS TYPE,*
FROM
    total_supply
UNION
SELECT
    'staked' AS TYPE,*
FROM
    staked
