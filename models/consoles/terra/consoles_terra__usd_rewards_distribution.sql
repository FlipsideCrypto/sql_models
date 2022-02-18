{{ config(
    materialized = 'view',
    unique_key = "CONCAT_WS('-', day, type)",
    tags = ['snowflake', 'terra', 'console']
) }}

WITH base_table AS (

    SELECT
        DATE AS DAY,
        SUM(balance) AS total_supply,
        SUM(case when balance_type = 'staked' then balance else 0 end) AS total_staked
    FROM
        {{ ref('terra__daily_balances') }}
    WHERE
        currency = 'LUNA'
        AND address != 'terra1fl48vsnmsdzcv85q5d2q4z5ajdha8yu3nln0mh'
        AND date::date >= current_date - 180
    GROUP BY
        DAY
)

select *
from base_table
unpivot (daily_total_supply for type in (total_supply, total_staked))