-- Velocity: 320abd12-4abf-4c05-a64a-a541f49e8c5c
{{ config(
    materialized = 'view',
    unique_key = 'date',
    tags = ['snowflake', 'terra', 'console_a']
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
