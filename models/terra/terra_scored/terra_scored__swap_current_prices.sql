{{ config(
  materialized = 'view',
  tags = ['snowflake', 'terra_views', 'terra', 'terrascored', 'swap_current_prices']
) }}

WITH terraswap_prices AS (
SELECT
block_timestamp,
date_trunc('hour',block_timestamp) AS hour,
CASE WHEN return_currency = 'uusd' THEN offer_currency
     WHEN offer_currency = 'uusd' THEN return_currency END AS token,
CASE WHEN return_currency = 'uusd' THEN return_amount / offer_amount 
     WHEN offer_currency = 'uusd' THEN offer_amount / return_amount END AS price,
'Terraswap' AS source
FROM {{ ref('terraswap__swaps') }}
WHERE block_timestamp > current_date - 2
  AND (return_currency = 'uusd' OR offer_currency = 'uusd')
  AND return_amount > 0 AND offer_amount > 0
  ),

astro_prices AS (
SELECT
block_timestamp,
date_trunc('hour',block_timestamp) AS hour,
CASE WHEN return_currency = 'uusd' THEN offer_currency
     WHEN offer_currency = 'uusd' THEN return_currency END AS token,
CASE WHEN return_currency = 'uusd' THEN return_amount / offer_amount 
     WHEN offer_currency = 'uusd' THEN offer_amount / return_amount END AS price,
'Astroport' AS source
FROM {{ ref('astroport__swaps') }}
WHERE block_timestamp > current_date - 2
  AND (return_currency = 'uusd' OR offer_currency = 'uusd')
  AND return_amount > 0 AND offer_amount > 0
),

native_prices AS (
SELECT
block_timestamp,
date_trunc('hour',block_timestamp) AS hour,
CASE WHEN ask_currency = 'UST' THEN offer_currency
     WHEN offer_currency = 'UST' THEN ask_currency END AS token,
CASE WHEN ask_currency = 'UST' THEN token_1_amount / offer_amount 
     WHEN offer_currency = 'UST' THEN offer_amount / token_1_amount END AS price,
'Native swap' AS source
FROM {{ ref('terra__swaps') }}
WHERE block_timestamp > current_date - 2
  AND (ask_currency = 'UST' OR offer_currency = 'UST')
  AND token_1_amount > 0 AND offer_amount > 0
),

symbols AS (
SELECT
symbol,
currency
FROM {{ ref('terra__oracle_prices') }}
GROUP BY symbol, currency
),

allprices AS (
SELECT
block_timestamp,
token,
price,
source
FROM terraswap_prices

UNION

SELECT
block_timestamp,
token,
price,
source
FROM astro_prices

UNION

SELECT
block_timestamp,
coalesce(s.currency, n.token) AS token, 
price,
source
FROM native_prices n
LEFT JOIN symbols s 
ON n.token = s.symbol
)

SELECT DISTINCT
last_value(token)  OVER (PARTITION BY token, source
                                  ORDER BY block_timestamp) AS token,
last_value(price) OVER (PARTITION BY token, source
                            ORDER BY block_timestamp) AS price,
source
FROM allprices