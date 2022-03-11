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

provide_msgs AS (
  SELECT
    m.blockchain,
    chain_id,
    block_id,
    msg_index,
    block_timestamp,
    tx_id,
    'provide_liquidity' AS event_type,
    msg_value :sender :: STRING AS sender,
    msg_value :contract :: STRING AS pool_address,
    l.address_name AS pool_name
  FROM
    {{ ref('silver_terra__msgs') }} m
  
  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
    ON msg_value :contract :: STRING = l.address 
    AND l.blockchain = 'terra' 
    AND l.creator = 'flipside'
  
  WHERE msg_value :execute_msg :provide_liquidity IS NOT NULL
    AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
  AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
{% endif %}
),

provide_events AS (
  SELECT
    msg_index,
    tx_id,
    event_attributes :assets [0] :amount / pow(10,6) AS token_0_amount,
    token_0_amount * o.price AS token_0_amount_usd,
    event_attributes :assets [0] :denom :: STRING AS token_0_currency,
    event_attributes :assets [1] :amount / pow(10,6) AS token_1_amount,
    token_1_amount * i.price AS token_1_amount_usd,
    event_attributes :assets [1] :denom :: STRING AS token_1_currency,
    event_attributes :share / pow(10,6) AS lp_share_amount,
    CASE
      WHEN event_attributes :"2_contract_address" :: STRING = 'terra17yap3mhph35pcwvhza38c2lkj7gzywzy05h7l0' THEN event_attributes :"4_contract_address" :: STRING
      ELSE coalesce(event_attributes :"2_contract_address" :: STRING, event_attributes :"1_contract_address" :: STRING)
    END AS lp_pool_address,
    l.address_name AS lp_pool_name
  FROM {{ ref('silver_terra__msg_events') }} t
    
  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
    ON event_attributes :"2_contract_address" :: STRING = l.address 
    AND l.blockchain = 'terra' 
    AND l.creator = 'flipside'
    
  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS r
    ON event_attributes :"4_contract_address" :: STRING = r.address 
    AND r.blockchain = 'terra' 
    AND r.creator = 'flipside'
    
  LEFT OUTER JOIN prices o
    ON DATE_TRUNC('hour', t.block_timestamp) = o.hour
    AND t.event_attributes :assets [0] :denom :: STRING = o.currency
    
  LEFT OUTER JOIN prices i
    ON DATE_TRUNC('hour',t.block_timestamp) = i.hour
    AND t.event_attributes :assets [1] :denom :: STRING = i.currency
  
  WHERE event_attributes :assets IS NOT NULL
    AND tx_id IN(SELECT DISTINCT tx_id FROM provide_msgs)
    AND event_type = 'from_contract'

  {% if is_incremental() %}
    AND t.block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
  {% endif %}
),

withdraw_msgs AS (
  SELECT
    m.blockchain,
    chain_id,
    block_id,
    msg_index,
    block_timestamp,
    tx_id,
    'withdraw_liquidity' AS event_type,
    msg_value :sender :: STRING AS sender,
    msg_value :contract :: STRING AS lp_pool_address,
    l.address_name AS lp_pool_name,
    msg_value :execute_msg :send :contract :: STRING AS pool_address,
    p.address_name AS pool_name
  FROM
    {{ ref('silver_terra__msgs') }} m

  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS p
    ON msg_value :execute_msg :send :contract :: STRING = p.address 
    AND p.blockchain = 'terra' 
    AND p.creator = 'flipside'
    
  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
    ON msg_value :contract :: STRING = l.address 
    AND l.blockchain = 'terra' 
    AND l.creator = 'flipside'
  
  WHERE msg_value :execute_msg :send :msg :withdraw_liquidity IS NOT NULL
    AND tx_status = 'SUCCEEDED'

  {% if is_incremental() %}
    AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
  {% endif %}
),
withdraw_events AS (
  SELECT
    msg_index,
    tx_id,
    event_attributes :refund_assets [0] :amount / pow(10,6) AS token_0_amount,
    token_0_amount * o.price AS token_0_amount_usd,
    event_attributes :refund_assets [0] :denom :: STRING AS token_0_currency,
    event_attributes :refund_assets [1] :amount / pow(10,6) AS token_1_amount,
    token_1_amount * i.price AS token_1_amount_usd,
    event_attributes :refund_assets [1] :denom :: STRING AS token_1_currency,
    event_attributes :withdrawn_share / pow(10,6) AS lp_share_amount
  FROM
    {{ ref('silver_terra__msg_events') }} t
    LEFT OUTER JOIN prices o
      ON DATE_TRUNC('hour', t.block_timestamp) = o.hour
      AND t.event_attributes :refund_assets [0] :denom :: STRING = o.currency
    
    LEFT OUTER JOIN prices i
      ON DATE_TRUNC('hour',t.block_timestamp) = i.hour
      AND t.event_attributes :refund_assets [1] :denom :: STRING = i.currency
  
  WHERE tx_id IN(SELECT tx_id FROM withdraw_msgs)
    AND event_type = 'from_contract'
    AND event_attributes :refund_assets IS NOT NULL

  {% if is_incremental() %}
    AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
  {% endif %}
) 

-- Provide Liquidity
SELECT
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  m.tx_id,
  event_type,
  sender,
  token_0_amount,
  token_0_amount_usd,
  token_0_currency,
  token_1_amount,
  token_1_amount_usd,
  token_1_currency,
  m.pool_address,
  pool_name,
  lp_share_amount,
  lp_pool_address,
  lp_pool_name
FROM provide_msgs m
  
JOIN provide_events e
  ON m.tx_id = e.tx_id
  AND m.msg_index = e.msg_index
  AND token_0_amount IS NOT NULL
  
UNION

  -- Remove Liquidity
SELECT
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  w.tx_id,
  event_type,
  sender,
  token_0_amount,
  token_0_amount_usd,
  token_0_currency,
  token_1_amount,
  token_1_amount_usd,
  token_1_currency,
  pool_address,
  pool_name,
  lp_share_amount,
  lp_pool_address,
  lp_pool_name
FROM withdraw_msgs w
  
JOIN withdraw_events we
  ON w.tx_id = we.tx_id
  AND w.msg_index = we.msg_index
