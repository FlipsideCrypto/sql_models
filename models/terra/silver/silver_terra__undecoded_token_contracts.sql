{{ config(
  materialized = 'incremental',
  unique_key = "token_contract",
  incremental_strategy = 'merge',
) }}

WITH wormhole_contracts AS (
SELECT
  event_attributes :contract_address :: STRING AS token_contract,
  'WORMHOLE' AS description,
  SYSDATE() AS _inserted_timestamp
FROM
  {{ ref("silver_terra__msg_events") }}
WHERE
  event_attributes :creator = 'terra10nmmwe8r3g99a9newtqa7a75xfgs2e8z87r2sf' --wormhole contracts
  AND block_timestamp > '2021-06-01'
),

oracle_feed_contracts AS (
SELECT DISTINCT
  msg_value :execute_msg :feed_price :prices [0] [0] :: STRING AS token_contract,
  'UNDECODED CW20' AS description,
  SYSDATE() AS _inserted_timestamp
FROM
  {{ ref("silver_terra__msgs") }}
WHERE 
  msg_value :execute_msg :feed_price IS NOT NULL
  AND block_timestamp::date >= current_date - 30
  AND token_contract <> 'terra15gwkyepfc6xgca5t5zefzwy42uts8l2m4g40k6' --MIR contract is decoded
),

decoded_contracts AS (
SELECT
v.value :contract_info :address::STRING AS decoded_contract
FROM
    {{ source(
      'bronze',
      'prod_terra_api'
    ) }}
, lateral flatten ( data :response_data) v
)

SELECT
token_contract,
description,
_inserted_timestamp
FROM wormhole_contracts
WHERE token_contract NOT IN (SELECT decoded_contract FROM decoded_contracts)

UNION ALL

SELECT
token_contract,
description,
_inserted_timestamp
FROM oracle_feed_contracts
WHERE token_contract NOT IN (SELECT decoded_contract FROM decoded_contracts)