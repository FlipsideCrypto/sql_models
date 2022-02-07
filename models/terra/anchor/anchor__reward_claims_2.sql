{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'anchor', 'reward_claims', 'address_labels']
) }}

WITH prices AS (

SELECT
  DATE_TRUNC('hour', block_timestamp) AS HOUR,
  currency,
  symbol,
  AVG(price_usd) AS price
FROM {{ ref('terra__oracle_prices') }}
WHERE 1 = 1

{% if is_incremental() %}
  AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
{% endif %}

GROUP BY 1,
         2,
         3

),

withdraw_msgs as (

SELECT 
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  msg_index,
  msg_value :sender :: STRING as sender,
  msg_value :contract :: STRING AS contract_address
FROM {{ ref('silver_terra__msgs') }} 
WHERE msg_value :execute_msg :withdraw IS NOT NULL
  AND msg_value :contract :: STRING = 'terra1897an2xux840p9lrh6py3ryankc6mspw49xse3'
  AND tx_status = 'SUCCEEDED'
    
{% if is_incremental() %}
  AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
{% endif %}    
    
),

reward_claim_msgs AS (

SELECT 
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  msg_index,
  msg_value :sender :: STRING as sender,
  msg_value :contract :: STRING AS contract_address
FROM {{ ref('silver_terra__msgs') }} 
WHERE msg_value:execute_msg:claim_rewards IS NOT NULL 
  AND msg_value:contract::string = 'terra1sepfj7s0aeg5967uxnfk4thzlerrsktkpelm5s'
  AND tx_status = 'SUCCEEDED'
    
{% if is_incremental() %}
  AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
{% endif %}  

),

all_rewards (

-- Staking Reward 
SELECT
  m.blockchain,
  m.chain_id,
  m.block_id,
  m.block_timestamp,
  m.tx_id,
  'staking_reward' AS action,
  m.sender,
  event_attributes :"0_amount" / pow(10,6) AS amount,
  event_attributes :"1_contract_address" :: STRING AS currency,
  m.contract_address
FROM {{ ref('silver_terra__msg_events') }} e
    
JOIN withdraw_msgs m
  ON m.tx_id = e.tx_id
  AND m.msg_index = e.msg_index

WHERE event_type = 'from_contract'
  AND tx_status = 'SUCCEEDED'
  AND event_attributes :"0_action" :: STRING = 'withdraw'

{% if is_incremental() %}
  AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
{% endif %}


UNION

-- Market Reward

SELECT
  m.blockchain,
  m.chain_id,
  m.block_id,
  m.block_timestamp,
  m.tx_id,
  'market_reward' AS action,
  m.sender,
  event_attributes :claim_amount / pow(10,6) AS amount,
  event_attributes :"2_contract_address" :: STRING AS currency,
  m.contract_address
FROM {{ ref('silver_terra__msg_events') }} e
    
JOIN reward_claim_msgs m 
  ON m.tx_id = e.tx_id
  AND m.msg_index = e.msg_index

WHERE event_type = 'from_contract'
  AND tx_status = 'SUCCEEDED'
  AND event_attributes :"0_action" :: STRING = 'claim_rewards'

{% if is_incremental() %}
  AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
{% endif %}

)

SELECT
  c.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  action,
  sender,
  amount
  amount * p0.price AS amount_usd,
  currency,
  contract_address,
  l0.address AS ontract_label,
FROM
  all_rewards c
  
  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l0
    ON contract_address = l0.address 
    AND l0.blockchain = 'terra' 
    AND l0.creator = 'flipside'  
  
  LEFT OUTER JOIN prices p0
    ON DATE_TRUNC( 'hour', block_timestamp) = p0.hour
    AND currency = p0.currency
  
  
  