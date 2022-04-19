{{ config(
  materialized = 'incremental',
  unique_key = "contract_address",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'terra_silver', 'dex_contracts', 'terra_contracts']
) }}

WITH create_events AS (
SELECT
me.tx_id,
me.block_timestamp,
event_attributes:"0_contract_address"::string AS factory_contract,
event_attributes:pair_contract_addr::string AS pool_address,
CASE 
  WHEN event_attributes:liquidity_token_addr::string IS NULL 
  THEN event_attributes:"0_liquidity_token_addr"::string 
  ELSE event_attributes:liquidity_token_addr::string 
  END AS lp_token,
split_part(event_attributes:pair::string, '-', 1) AS token1,
split_part(event_attributes:pair::string, '-', 2) AS token2,
project_name AS dex_name
FROM {{ ref('silver_terra__msg_events') }} me
JOIN {{ ref('silver_crosschain__address_labels') }} al ON me.event_attributes:"0_contract_address"::string = al.address
WHERE
event_type = 'wasm'
AND (event_attributes:action = 'create_pair' OR event_attributes:"0_action" = 'create_pair')

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
)
SELECT
ce.tx_id,
ce.block_timestamp,
pool_address AS contract_address,
NULL AS decimals,
concat(dex_name, ' LP pool') AS contract_name,
CASE
WHEN substr(token1, 1, 5) = 'terra'
THEN concat(cm1.symbol, '/', token2)
ELSE concat(token1, '/', cm2.symbol)
END AS symbol

FROM create_events ce
LEFT OUTER JOIN {{ ref('silver_terra__contract_metadata') }} cm1 ON ce.token1 = cm1.contract_address
LEFT OUTER JOIN {{ ref('silver_terra__contract_metadata') }} cm2 ON ce.token2 = cm2.contract_address

UNION

SELECT
ce.tx_id,
ce.block_timestamp,
lp_token AS contract_address,
6 AS decimals,
concat(dex_name, ' LP token') AS contract_name,
CASE
WHEN substr(token1, 1, 5) = 'terra'
THEN concat(cm1.symbol, '/', token2)
ELSE concat(token1, '/', cm2.symbol)
END AS symbol

FROM create_events ce
LEFT OUTER JOIN {{ ref('silver_terra__contract_metadata') }} cm1 ON ce.token1 = cm1.contract_address
LEFT OUTER JOIN {{ ref('silver_terra__contract_metadata') }} cm2 ON ce.token2 = cm2.contract_address

WHERE lp_token IS NOT NULL