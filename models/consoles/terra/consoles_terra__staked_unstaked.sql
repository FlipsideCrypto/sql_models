{{ config(
    materialized = 'view',
    unique_key = "CONCAT_WS('-', DAYZZ, BALANCE_TYPE)",
    tags = ['snowflake', 'terra', 'console']
) }}

select
    date_trunc('day', date) AS dayzz,
    sum(BALANCE) as bal,
    sum(BALANCE_USD) as balus,
    BALANCE_TYPE
from
    {{ ref('terra__daily_balances') }}
where
    dayzz >= CURRENT_DATE - interval '365 days'
    and CURRENCY = 'LUNA'
    and address <> 'terra1fl48vsnmsdzcv85q5d2q4z5ajdha8yu3nln0mh'
group by
    dayzz,
    BALANCE_TYPE 
order by
    dayzz desc