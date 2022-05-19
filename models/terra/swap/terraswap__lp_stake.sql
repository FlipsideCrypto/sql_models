{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'terraswap', 'lp', 'address_labels']
) }}

-- LP Un-staking
WITH unstaking AS (

   SELECT
    a.blockchain,
    a.chain_id,
    a.block_id,
    a.msg_index,
    a.block_timestamp,
    a.tx_id,
    'unstake_lp' AS event_type,
    coalesce(action_log :staker_addr :: STRING, msg_value :sender::STRING) AS sender,
    action_log :amount / POW(10,6) AS amount,
    action_log :asset_token::STRING AS contract_address
  FROM
    {{ ref('silver_terra__event_actions') }} a
  LEFT JOIN {{ ref('silver_terra__msgs') }} m
   ON a.tx_id = m.tx_id
   AND a.msg_index = m.msg_index
  WHERE action_method= 'unbond'

  {% if is_incremental() %}
    AND a.block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
  {% endif %}
),


staking AS (
  -- stake
 SELECT
    a.blockchain,
    a.chain_id,
    a.block_id,
    a.msg_index,
    a.block_timestamp,
    a.tx_id,
    'stake_lp' AS event_type,
    coalesce(action_log :owner :: STRING, msg_value :sender::STRING) AS sender,
    action_log :amount / POW(10,6) AS amount,
    msg_value :contract::STRING AS contract_address
  FROM
    {{ ref('silver_terra__event_actions') }} a
   LEFT JOIN {{ ref('silver_terra__msgs') }} m
   ON a.tx_id = m.tx_id
   AND a.msg_index = m.msg_index
  WHERE action_method =  'bond'

{% if is_incremental() %}
  AND a.block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
{% endif %}
),

-- LPs
lps AS (
SELECT
contract_address AS lp_token
FROM {{ ref('silver_terra__dex_contracts') }}
WHERE contract_name = 'astroport LP token'                              
  ),

re_investing AS (
SELECT
m.blockchain,
m.chain_id,
m.block_id,
m.block_timestamp,
m.tx_id,
'stake_lp' AS event_type,
msg_value :sender :: STRING AS sender,
event_attributes :share / pow(10,6) AS amount,
event_attributes :staking_token::STRING AS contract_address
FROM {{ ref('silver_terra__msgs') }} m
LEFT JOIN {{ ref('silver_terra__msg_events') }} e
ON m.tx_id = e.tx_id
AND m.msg_index = e.msg_index
WHERE msg_value :contract::STRING = 'terra1kehar0l76kzuvrrcwj5um72u3pjq2uvp62aruf' --Mirror farm
AND m.tx_status = 'SUCCEEDED'
AND msg_value :execute_msg :re_invest :asset_token IS NOT NULL
AND contract_address NOT IN (SELECT lp_token FROM lps)
AND e.event_type = 'from_contract'

{% if is_incremental() %}
  AND m.block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ this }})
{% endif %}

),

msgs_zap_out_of_strategy AS (
SELECT
blockchain,
chain_id,
block_id,
block_timestamp,
tx_id,
msg_index,
'unstake_lp' AS event_type,
msg_value :sender :: STRING AS sender,
msg_value :execute_msg :zap_out_of_strategy :amount  / pow(10,6) AS amount
FROM {{ ref('silver_terra__msgs') }} 
WHERE msg_value :contract::STRING IN ('terra1g7jjjkt5uvkjeyhp8ecdz4e4hvtn83sud3tmh2', 'terra1leadedadm2fguezmd4e445h6fe337yzq8n2dxf') --Apollo and Angel contracts
AND msg_value :execute_msg :zap_out_of_strategy :strategy_id IS NOT NULL
AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
  AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{this}} )
{% endif %}
 ),

zap_outs AS (
SELECT
m.blockchain,
m.chain_id,
m.block_id,
m.block_timestamp,
m.tx_id,
m.event_type,
sender,
amount,
event_attributes :staking_token::STRING AS contract_address
FROM msgs_zap_out_of_strategy m
LEFT JOIN {{ ref('silver_terra__msg_events') }} e
ON m.tx_id = e.tx_id AND m.msg_index = e.msg_index
WHERE event_attributes :staking_token::STRING NOT IN (SELECT lp_token FROM lps)
AND e.event_type = 'from_contract'
),

auto_stake AS (
SELECT
e.blockchain,
e.chain_id,
e.block_id,
e.block_timestamp,
e.tx_id,
'stake_lp' AS event_type,
msg_value :sender :: STRING AS sender,
event_attributes :share / pow(10,6) AS amount,
event_attributes :"5_contract_address"::STRING AS contract_address
FROM {{ ref('silver_terra__msg_events') }}  e
LEFT JOIN {{ ref('silver_terra__msgs') }} m
ON e.tx_id = m.tx_id 
AND e.msg_index = m.msg_index
WHERE event_attributes :"0_action"::STRING = 'auto_stake'
AND event_attributes :"5_action"::STRING = 'mint'
AND contract_address NOT IN (SELECT lp_token FROM lps)
AND e.event_type = 'from_contract'
AND e.tx_status = 'SUCCEEDED'

{% if is_incremental() %}
  AND e.block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ this }})
{% endif %}
),


msgs_zap_into_strategy AS (
SELECT
blockchain,
chain_id,
block_id,
block_timestamp,
tx_id,
'stake_lp' AS event_type,
msg_value :sender :: STRING AS sender
FROM {{ ref('silver_terra__msgs') }}
WHERE msg_value :contract::STRING  IN ('terra1g7jjjkt5uvkjeyhp8ecdz4e4hvtn83sud3tmh2', 'terra1leadedadm2fguezmd4e445h6fe337yzq8n2dxf') --Apollo and Angel contracts
AND msg_value :execute_msg :zap_into_strategy :strategy_id IS NOT NULL
AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
  AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ this }})
{% endif %}
),

zap_ins AS (
SELECT
m.blockchain,
m.chain_id,
m.block_id,
m.block_timestamp,
m.tx_id,
m.event_type,
sender,
event_attributes :share / pow(10,6) AS amount,
event_attributes :lp_token::STRING AS contract_address
FROM msgs_zap_into_strategy m
LEFT JOIN {{ ref('silver_terra__msg_events') }} e
ON m.tx_id = e.tx_id
WHERE event_attributes :lp_token::STRING NOT IN (SELECT lp_token FROM lps)
AND e.event_type = 'from_contract'
)

SELECT DISTINCT * FROM (
SELECT
  u.blockchain,
  u.chain_id,
  block_id,
  block_timestamp,
  tx_id,
  event_type,
  sender,
  amount,
  contract_address,
  l.address_name AS contract_label
FROM
  unstaking u
LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} l 
  ON contract_address = l.address 
  AND l.blockchain = 'terra' 
  AND l.creator = 'flipside'
WHERE contract_address NOT IN (SELECT lp_token FROM lps)

UNION ALL

SELECT
  s.blockchain,
  s.chain_id,
  block_id,
  block_timestamp,
  tx_id,
  event_type,
  sender,
  amount,
  contract_address,
  l.address_name AS contract_label
FROM
  staking s
LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} l 
  ON contract_address = l.address 
  AND l.blockchain = 'terra' 
  AND l.creator = 'flipside'
WHERE contract_address NOT IN (SELECT lp_token FROM lps)

UNION ALL

SELECT
  r.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  r.tx_id,
  event_type,
  sender,
  amount,
  contract_address,
  l.address_name AS contract_label
FROM
  re_investing r

LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} l 
  ON contract_address = l.address 
  AND l.blockchain = 'terra' 
  AND l.creator = 'flipside'

UNION ALL

SELECT
  o.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  o.tx_id,
  event_type,
  sender,
  amount,
  contract_address,
  l.address_name AS contract_label
FROM
  zap_outs o

LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} l 
  ON contract_address = l.address 
  AND l.blockchain = 'terra' 
  AND l.creator = 'flipside'

UNION ALL

SELECT
  a.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  a.tx_id,
  event_type,
  sender,
  amount,
  contract_address,
  l.address_name AS contract_label
FROM
  auto_stake a

LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} l 
  ON contract_address = l.address 
  AND l.blockchain = 'terra' 
  AND l.creator = 'flipside'

UNION ALL

SELECT
  i.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  i.tx_id,
  event_type,
  sender,
  amount,
  contract_address,
  l.address_name AS contract_label
FROM
  zap_ins i

LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} l 
  ON contract_address = l.address 
  AND l.blockchain = 'terra' 
  AND l.creator = 'flipside'
)
WHERE sender IS NOT NULL
AND amount IS NOT NULL
AND contract_address NOT IN ('uluna', 'uusd')