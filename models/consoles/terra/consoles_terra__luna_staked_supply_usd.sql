{{ config(
    materialized = 'view',
    unique_key = 'date',
    tags = ['snowflake', 'terra', 'console']
) }}

SELECT
    DATE,
    SUM(balance_usd) AS staked_supply_usd
FROM
    {{ ref('terra__daily_balances') }}
WHERE
    currency = 'LUNA'
    AND balance_type = 'staked'
GROUP BY
    DATE
ORDER BY
    DATE DESC
