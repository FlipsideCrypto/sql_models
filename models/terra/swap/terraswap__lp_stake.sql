{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'terraswap', 'lp', 'address_labels']
) }}

-- LP Un-staking
WITH msgs AS (

  SELECT
    blockchain,
    chain_id,
    block_id,
    msg_index,
    block_timestamp,
    tx_id,
    'unstake_lp' AS event_type,
    msg_value :sender :: STRING AS sender,
    coalesce(msg_value :execute_msg :unbond :amount, msg_value :execute_msg :unbond :tokens, 
              msg_value :execute_msg :unbond :asset :amount, 0) / pow(10,6) AS amount
  FROM
    {{ ref('silver_terra__msgs') }}
  WHERE msg_value :execute_msg :unbond IS NOT NULL
    AND tx_status = 'SUCCEEDED'

  {% if is_incremental() %}
    AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
  {% endif %}
),

events AS (
  SELECT
    msg_index,
    tx_id,
    coalesce(event_attributes :"0_contract_address" :: STRING, event_attributes :contract_address :: STRING ) AS contract_address
  FROM {{ ref('silver_terra__msg_events') }}
  WHERE tx_id IN(SELECT DISTINCT tx_id FROM msgs)
    AND event_type = 'execute_contract'
    AND msg_index = 0
    AND contract_address IS NOT NULL

  {% if is_incremental() %}
    AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
  {% endif %}

),

bonding_unbonding AS (
-- unstake
SELECT
  m.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  m.tx_id,
  event_type,
  sender,
  amount,
  contract_address,
  l.address_name AS contract_label
FROM
  msgs m
  
JOIN events e
  ON m.tx_id = e.tx_id
  AND m.msg_index = e.msg_index

LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} l 
  ON contract_address = l.address 
  AND l.blockchain = 'terra' 
  AND l.creator = 'flipside'

UNION

  -- stake
SELECT
  m.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  'stake_lp' AS event_type,
  msg_value :sender :: STRING AS sender,
  msg_value :execute_msg :send :amount / pow(10,6) AS amount,
  msg_value :contract :: STRING AS contract_address,
  address_name AS contract_label
FROM {{ ref('silver_terra__msgs') }} m
  
LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} l 
  ON msg_value :contract :: STRING = l.address 
  AND l.blockchain = 'terra' 
  AND l.creator = 'flipside'
  
WHERE msg_value :execute_msg :send :msg :bond IS NOT NULL
  AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
  AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
{% endif %}
),

--terraswap LPs
lps AS (
SELECT
event_attributes :liquidity_token_addr::STRING AS lp_token
FROM {{ ref('silver_terra__msgs') }} m
LEFT JOIN {{ ref('silver_terra__msg_events') }} e
ON m.tx_id = e.tx_id
WHERE event_type = 'from_contract'
AND event_attributes :liquidity_token_addr IS NOT NULL
AND msg_value :contract::STRING in ( 'terra1mzj9nsxx0lxlaxnekleqdy8xnyw2qrh3uz6h8p', --Mirror factory
                                    'terra1ulgw0td86nvs4wtpsc80thv6xelk76ut7a7apj') --Terraswap token factory                                
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
AND contract_address in (SELECT lp_token FROM lps)
AND e.event_type = 'from_contract'

{% if is_incremental() %}
  AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
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
  AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
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
WHERE event_attributes :staking_token::STRING IN (SELECT lp_token FROM lps)
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
AND contract_address in (SELECT lp_token FROM lps)
AND e.event_type = 'from_contract'
AND e.tx_status = 'SUCCEEDED'

{% if is_incremental() %}
  AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
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
  AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
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
WHERE event_attributes :lp_token::STRING IN (SELECT lp_token FROM lps)
AND e.event_type = 'from_contract'
),

--Spectrum and Apollo staking
classic_staking AS (
SELECT
msg_value :execute_msg :send :msg::STRING AS MSG,
try_parse_json(try_base64_decode_string(MSG)) AS decoded,
blockchain,
chain_id,
block_id,
block_timestamp,
tx_id,
'stake_lp' AS event_type,
msg_value :sender :: STRING AS sender,
msg_value :execute_msg :send :amount / pow(10,6) AS amount,
msg_value :contract :: STRING AS contract_address
FROM {{ ref('silver_terra__msgs') }}
WHERE (decoded :bond :asset_token::STRING IS NOT NULL OR decoded :deposit :strategy_id IS NOT NULL)
AND contract_address IN (SELECT lp_token FROM lps)
AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
  AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
{% endif %}
)

SELECT
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  event_type,
  sender,
  amount,
  contract_address,
  contract_label
FROM
  bonding_unbonding
WHERE contract_address IN (SELECT lp_token FROM lps)

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

UNION ALL

SELECT
  s.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  s.tx_id,
  event_type,
  sender,
  amount,
  contract_address,
  l.address_name AS contract_label
FROM
  classic_staking s

LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} l 
  ON contract_address = l.address 
  AND l.blockchain = 'terra' 
  AND l.creator = 'flipside'