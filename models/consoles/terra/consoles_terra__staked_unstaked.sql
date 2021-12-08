{{ config(
    materialized = 'view',
    unique_key = "CONCAT_WS('-', DAYZZ, BALANCE_TYPE)",
    tags = ['snowflake', 'terra', 'console']
) }}

SELECT
    DATE_TRUNC(
        'day',
        DATE
    ) AS dayzz,
    SUM(balance) AS bal,
    SUM(balance_usd) AS balus,
    balance_type
FROM
    {{ ref('terra__daily_balances') }}
WHERE
    dayzz >= CURRENT_DATE - INTERVAL '365 days'
    AND currency = 'LUNA'
    AND address <> 'terra1fl48vsnmsdzcv85q5d2q4z5ajdha8yu3nln0mh'
GROUP BY
    dayzz,
    balance_type
ORDER BY
    dayzz DESC
