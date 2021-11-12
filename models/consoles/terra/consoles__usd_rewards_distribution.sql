-- Velocity: 115cafb9-4e82-4967-9c43-906b706760e4
{{ config(
    materialized = 'incremental',
    unique_key = 'day',
    incremental_strategy = 'delete+insert',
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

{% if is_incremental() %}
AND DAY >= (
    SELECT
        MAX(
            DATE
        )
    FROM
        {{ ref('terra__daily_balances') }}
)
{% endif %}
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

{% if is_incremental() %}
AND DAY >= (
    SELECT
        MAX(
            DATE
        )
    FROM
        {{ ref('terra__daily_balances') }}
)
{% endif %}
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
