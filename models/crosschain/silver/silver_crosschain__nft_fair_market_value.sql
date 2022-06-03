{{ config(
  materialized = 'table',
  tags = ['snowflake', 'crosschain', 'nft', 'nft_deal_score']
) }}

WITH base AS (
  SELECT * 
  FROM {{ source('bronze', 'prod_data_science_uploads_1748940988') }}
  WHERE record_content[0]:collection IS NOT NULL
  AND record_metadata:key like '%nft-deal-score-rankings-%'
), base2 AS (
  SELECT t.value:collection::string AS collection
  , t.value:cur_floor AS old_floor
  , t.value:cur_sd AS old_sd
  , t.value:deal_score_rank AS deal_score_rank
  , t.value:fair_market_price AS old_fair_market_price
  , t.value:lin_coef AS lin_coef
  , t.value:log_coef AS log_coef
  , t.value:rarity_rank AS rarity_rank
  , t.value:token_id AS token_id
  , ROW_NUMBER() OVER (PARTITION BY collection, token_id ORDER BY record_metadata:CreateTime DESC) AS rn
  FROM base
  , LATERAL FLATTEN(
  input => record_content
  ) t
), base3 AS (
  SELECT *
  FROM {{ source('bronze', 'prod_data_science_uploads_1748940988') }}
  WHERE record_content[0]:collection IS NOT NULL
  AND record_metadata:key like '%nft-deal-score-floors-%'
), base4 AS (
  SELECT t.value:collection::string AS collection
  , t.value:cur_floor AS new_floor
  , b.*
  , ROW_NUMBER() OVER (PARTITION BY collection ORDER BY record_metadata:CreateTime DESC) AS rn
  FROM base3 b
  , LATERAL FLATTEN(
  input => record_content
  ) t
), metadata AS (
  SELECT project_name AS collection
  , token_id
  , mint
  , ROW_NUMBER() OVER (PARTITION BY collection, token_id ORDER BY created_at_timestamp DESC) AS rn
  FROM {{ source('solana', 'dim_nft_metadata') }}
  WHERE mint IS NOT NULL
), base5 AS (
  SELECT b2.*
  , m.mint
  , b4.new_floor
  FROM base2 b2
  JOIN base4 b4 ON b2.collection = b4.collection AND b4.rn = 1
  LEFT JOIN metadata m ON m.token_id = b2.token_id AND m.collection = b2.collection AND m.rn = 1
  WHERE b2.rn = 1
), base6 AS (
  SELECT *
  , old_sd * new_floor / old_floor AS cur_sd
  , old_fair_market_price + ((new_floor - old_floor) * lin_coef) + (( new_floor - old_floor) * log_coef * old_fair_market_price / old_floor) AS new_fair_market_price
  FROM base5
)
SELECT collection
, mint
, token_id
, deal_score_rank
, rarity_rank
, new_floor AS floor_price
, ROUND(CASE WHEN new_fair_market_price < floor_price THEN floor_price ELSE new_fair_market_price END, 2) AS fair_market_price
, ROUND(CASE WHEN new_fair_market_price - cur_sd < floor_price * 0.975 THEN floor_price * 0.975 ELSE new_fair_market_price - cur_sd END, 2) AS price_low
, ROUND(CASE WHEN new_fair_market_price + cur_sd < floor_price * 1.025 THEN floor_price * 1.025 ELSE new_fair_market_price + cur_sd END, 2) AS price_high
FROM base6

