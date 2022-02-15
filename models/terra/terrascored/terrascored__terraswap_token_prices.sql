{{ config(
  materialized = 'view',
  tags = ['snowflake', 'terra_views', 'terra', 'terrascored', 'terraswap_token_prices']
) }}

WITH allprices AS (
SELECT
block_timestamp,
date_trunc('hour',block_timestamp) AS hour,
CASE WHEN return_currency = 'uusd' THEN offer_currency
     WHEN offer_currency = 'uusd' THEN return_currency END AS token,
CASE WHEN return_currency = 'uusd' THEN return_amount / offer_amount 
     WHEN offer_currency = 'uusd' THEN offer_amount / return_amount END AS price
FROM {{ ref('terraswap__swaps') }}
WHERE block_timestamp > current_date - 2
  AND (return_currency = 'uusd' OR offer_currency = 'uusd')
  AND return_amount > 0 AND offer_amount > 0
  )

SELECT DISTINCT
last_value(token)  OVER (PARTITION BY token --, hour 
                                  ORDER BY block_timestamp) AS token,
last_value(price) OVER (PARTITION BY token --, hour 
                            ORDER BY block_timestamp) AS price
FROM allprices