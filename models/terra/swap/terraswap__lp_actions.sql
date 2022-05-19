{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'terraswap', 'terraswap_lp_actions', 'address_labels']
) }}

WITH prices AS (

  SELECT
    DATE_TRUNC('hour', block_timestamp) AS HOUR,
    currency,
    symbol,
    AVG(price_usd) AS price
  FROM
    {{ ref('terra__oracle_prices') }}
  WHERE
    1 = 1

{% if is_incremental() %}
  AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
{% endif %}

GROUP BY
  1,
  2,
  3
),

astro_pairs AS (
  SELECT 
    tx_id,
    event_attributes :pair_contract_addr::STRING AS contract_address
  FROM {{ ref('silver_terra__msg_events') }}
  where tx_id in (SELECT
                  tx_id
                  from terra.msgs
                  where msg_value :execute_msg :create_pair IS NOT NULL
  				and msg_value :contract = 'terra1fnywlw4edny3vw44x04xd67uzkdqluymgreu7g'
                 )
  AND event_type = 'from_contract'
),

provide_events AS (
SELECT
    a.blockchain,
    a.chain_id,
    a.block_id,
    a.block_timestamp,
    a.tx_id,
    a.msg_index,
    'provide_liquidity' AS event_type,
    coalesce(action_log :sender::STRING, msg_value :sender::STRING) AS sender,
    regexp_substr(split_part(action_log :assets, ',', 0), '[0-9]+') / POW(10,6) AS token_0_amount,
    regexp_substr(split_part(action_log :assets, ',', 0),'(u|t)[a-z0-9]+', 1) AS token_0_currency,
    regexp_substr(split_part(action_log :assets, ',', 2), '[0-9]+') / POW(10,6) AS token_1_amount,
    regexp_substr(split_part(action_log :assets, ',', 2),'(u|t)[a-z0-9]+', 1) AS token_1_currency,
    action_log :share / pow(10,6) AS lp_share_amount,
    action_contract_address AS pool_address
  FROM {{ ref('silver_terra__event_actions') }} a
  LEFT JOIN {{ ref('silver_terra__msgs') }} m
  ON a.tx_id = m.tx_id
  AND a.msg_index = m.msg_index
  WHERE action_method = 'provide_liquidity'

   {% if is_incremental() %}
  AND a.block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
  {% endif %}
  ),
  
mints AS (
SELECT
tx_id,
msg_index,
action_contract_address AS lp_pool_address,
action_log :amount /POW(10,6) AS amount
FROM {{ ref('silver_terra__event_actions') }}
WHERE action_method = 'mint'
AND tx_id in (SELECT DISTINCT tx_id FROM provide_events)
),

withdraw_events AS (
SELECT
    a.blockchain,
    a.chain_id,
    a.block_id,
    a.block_timestamp,
    a.tx_id,
    a.msg_index,
    'withdraw_liquidity' AS event_type,
    msg_value :sender::STRING AS sender,
    regexp_substr(split_part(action_log :refund_assets, ',', 0), '[0-9]+') / POW(10,6) AS token_0_amount,
    regexp_substr(split_part(action_log :refund_assets, ',', 0),'(u|t)[a-z0-9]+', 1) AS token_0_currency,
    regexp_substr(split_part(action_log :refund_assets, ',', 2), '[0-9]+') / POW(10,6) AS token_1_amount,
     regexp_substr(split_part(action_log :refund_assets, ',', 2),'(u|t)[a-z0-9]+', 1) AS token_1_currency,
    action_log :withdrawn_share / pow(10,6) AS lp_share_amount,
    action_contract_address AS pool_address
  FROM {{ ref('silver_terra__event_actions') }} a
  LEFT JOIN {{ ref('silver_terra__msgs') }} m
  ON a.tx_id = m.tx_id
  AND a.msg_index = m.msg_index
  WHERE action_method = 'withdraw_liquidity'
  
  {% if is_incremental() %}
  AND a.block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
  {% endif %}
  ),
  
burns AS (
SELECT
tx_id,
msg_index,
action_contract_address AS lp_pool_address,
action_log :amount /POW(10,6) AS amount
FROM {{ ref('silver_terra__event_actions') }}
WHERE action_method = 'burn'
AND tx_id in (SELECT DISTINCT tx_id FROM withdraw_events)
)

SELECT DISTINCT * FROM (
SELECT
p.blockchain,
p.chain_id,
p.block_id,
p.block_timestamp,
p.tx_id,
event_type,
sender,
token_0_amount,
token_0_amount * o.price AS token_0_amount_usd,
token_0_currency,
token_1_amount,
token_1_amount * r.price AS token_1_amount_usd,
token_1_currency,
pool_address,
l.address_name AS pool_name,
lp_share_amount,
lp_pool_address,
la.address_name AS lp_pool_name
FROM provide_events p
LEFT JOIN mints m
ON p.tx_id = m.tx_id
AND p.msg_index = m.msg_index
AND p.lp_share_amount = m.amount
LEFT OUTER JOIN prices o
ON DATE_TRUNC('hour',p.block_timestamp) = o.hour
AND p.token_0_currency = o.currency
LEFT OUTER JOIN prices r
ON DATE_TRUNC('hour',p.block_timestamp) = r.hour
AND p.token_1_currency = r.currency
LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} l 
  ON pool_address = l.address 
  AND l.blockchain = 'terra' 
  AND l.creator = 'flipside'
LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} la 
  ON lp_pool_address = la.address 
  AND la.blockchain = 'terra' 
  AND la.creator = 'flipside'
WHERE pool_address NOT IN (SELECT contract_address from astro_pairs) --remove astroport swaps
AND token_0_currency IS NOT NULL
AND token_1_currency IS NOT NULL
AND sender IS NOT NULL

UNION ALL

SELECT
w.blockchain,
w.chain_id,
w.block_id,
w.block_timestamp,
w.tx_id,
event_type,
sender,
token_0_amount,
token_0_amount * o.price AS token_0_amount_usd,
token_0_currency,
token_1_amount,
token_1_amount * r.price AS token_1_amount_usd,
token_1_currency,
pool_address,
l.address_name AS pool_name,
lp_share_amount,
lp_pool_address,
la.address_name AS lp_pool_name
FROM withdraw_events w
LEFT JOIN burns b
ON w.tx_id = b.tx_id
AND w.msg_index = b.msg_index
AND w.lp_share_amount = b.amount
LEFT OUTER JOIN prices o
ON DATE_TRUNC('hour',w.block_timestamp) = o.hour
AND w.token_0_currency = o.currency
LEFT OUTER JOIN prices r
ON DATE_TRUNC('hour',w.block_timestamp) = r.hour
AND w.token_1_currency = r.currency
LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} l 
  ON pool_address = l.address 
  AND l.blockchain = 'terra' 
  AND l.creator = 'flipside'
LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} la 
  ON lp_pool_address = la.address 
  AND la.blockchain = 'terra' 
  AND la.creator = 'flipside'
WHERE pool_address NOT IN (SELECT contract_address from astro_pairs) --remove astroport swaps
AND token_0_currency IS NOT NULL
AND token_1_currency IS NOT NULL
AND sender IS NOT NULL
)