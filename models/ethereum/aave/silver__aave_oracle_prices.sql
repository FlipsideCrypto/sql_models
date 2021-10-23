{{
  config(
    materialized='incremental',
    unique_key='hour || token_address',
    incremental_strategy='delete+insert',
    tags=['snowflake', 'ethereum', 'aave', 'aave_oracle_prices']
  )
}}


WITH 
-- get decimals, preferably from contract reads but using the prices table as a fallback
decimals_raw as (

    SELECT address AS token_address,
    meta:decimals AS decimals,name,
    2 as weight
    FROM {{ref('silver_ethereum__contracts')}}
    WHERE meta:decimals IS NOT NULL

    UNION

    SELECT DISTINCT token_address,
    decimals,symbol AS name,
    1 AS weight
    FROM {{ref('ethereum__token_prices_hourly')}}
    WHERE 
    decimals IS NOT NULL

), decimals AS (
  SELECT token_address,decimals,name
  FROM decimals_raw
  QUALIFY (row_number() OVER (partition by token_address order by weight desc)) = 1
),
-- implementing aave oracle prices denominated in wei
oracle AS (
    SELECT
        date_trunc('hour',block_timestamp) AS hour,
        LOWER(inputs:address::string) AS token_address,
        MEDIAN(value_numeric) AS value_ethereum -- values are given in wei and need to be converted to ethereum
    FROM
        {{ref('silver_ethereum__reads')}}
    WHERE 
        contract_address = '0xa50ba011c48153de246e5192c8f9258a2ba79ca9' -- check if there is only one oracle
        
        AND block_timestamp::date >= CURRENT_DATE - 720
        
    
        
  GROUP BY 1,2
)

--pull hourly prices for each underlying
SELECT
    DISTINCT
        o.hour AS hour,
        dc.decimals AS decimals,
        AVG((o.value_ethereum) * p.price/POWER(10,18 - dc.decimals)) AS price, -- this is all to get price in wei to price in USD
        o.token_address,
        '0xa50ba011c48153de246e5192c8f9258a2ba79ca9' AS oracle_contract
FROM
oracle o
INNER JOIN {{ref('ethereum__token_prices_hourly')}} p
    ON o.hour = p.hour
    AND o.hour >= CURRENT_DATE - 720
    AND p.token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
LEFT JOIN decimals_raw dc 
    ON o.token_address = dc.token_address
GROUP BY o.hour,o.token_address,dc.decimals
ORDER BY 1 DESC
