{{ config(
  materialized = 'incremental',
  unique_key = "contract_address",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'terra_silver', 'contract_metadata', 'terra_contracts']
) }}

WITH contract_events AS (

  SELECT
    block_timestamp,
    tx_id,
    msg_index,
    event_attributes :contract_address :: STRING AS contract_address
  FROM
    {{ ref('silver_terra__msg_events') }}
  WHERE
    event_type = 'instantiate_contract'
    AND event_attributes :contract_address IS NOT NULL

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}

UNION

  SELECT
    block_timestamp,
    tx_id,
    msg_index,
    e.value::STRING AS contract_address
  FROM
    {{ ref('silver_terra__msg_events') }},
  lateral flatten (input => event_attributes) e
  WHERE
    event_type = 'instantiate_contract'
  AND e.key LIKE  '%_contract_address'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}

),

contract_msgs AS (
  SELECT
    tm.block_timestamp,
    tm.tx_id,
    contract_address,
    msg_value :init_msg :decimals AS decimals,
    msg_value :init_msg :name :: STRING AS contract_name,
    msg_value :init_msg :symbol :: STRING AS symbol,
    FALSE AS is_wormhole
  FROM
    {{ ref('silver_terra__msgs') }}
    tm
    JOIN contract_events ce
    ON tm.tx_id = ce.tx_id
    AND tm.msg_index = ce.msg_index
  WHERE
     msg_type IN ('wasm/MsgInstantiateContract', 'wasm/MsgExecuteContract')
     AND (msg_value :execute_msg :execute :proposal_id IS NOT NULL OR msg_value :init_msg IS NOT NULL)
     

{% if is_incremental() %}
AND tm.block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
),

wormhole_txs AS (
  SELECT
    block_timestamp,
    tx_id,
    event_attributes :contract_address :: STRING AS token_contract,
    TRUE AS is_wormhole
  FROM
    {{ ref("silver_terra__msg_events") }}
  WHERE
    event_attributes :creator = 'terra10nmmwe8r3g99a9newtqa7a75xfgs2e8z87r2sf' --wormhole contracts
    AND block_timestamp > '2021-06-01'
),

mASSETS AS (
SELECT 
  m.block_timestamp,
  m.tx_id,
  event_attributes :"0_asset_token"::STRING AS contract_address
FROM 
  {{ ref("silver_terra__msgs") }} m
  LEFT JOIN {{ ref("silver_terra__msg_events") }} e 
  ON m.tx_id = e.tx_id
  AND m.msg_index = e.msg_index
WHERE 
  msg_value :contract::STRING = 'terra1mzj9nsxx0lxlaxnekleqdy8xnyw2qrh3uz6h8p' --MIR factory
  AND msg_value :execute_msg :whitelist IS NOT NULL
  AND event_type = 'from_contract'

UNION ALL
  
SELECT 
  block_timestamp,
  tx_id,
  event_attributes :"1_asset_token"::STRING AS contract_address
FROM 
  {{ ref("silver_terra__msg_events") }}
WHERE
  event_attributes :"1_action"::STRING = 'whitelist'
  AND event_attributes :"1_contract_address"::STRING = 'terra1mzj9nsxx0lxlaxnekleqdy8xnyw2qrh3uz6h8p'
),

decoded_contracts AS (
SELECT
  tx_id,
  block_timestamp,
  i.address AS contract_address,
  init_msg :decimals :: numeric AS decimals,
  init_msg :name :: STRING AS contract_name,
  init_msg :symbol :: STRING AS symbol,
  is_wormhole
FROM
    {{ ref('silver_terra__contract_info') }} i
    LEFT JOIN wormhole_txs w
    ON i.address = w.token_contract
WHERE
    contract_address NOT IN (
      'terra1kc87mu460fwkqte29rquh4hc20m54fxwtsx7gp',
      'terra17wgmccdu57lx09yzhnnev39srqj7msg9ky2j76',
      'terra1dzhzukyezv0etz22ud940z7adyv7xgcjkahuun',
      'terra1xj49zyqrwpv5k928jwfpfy2ha668nwdgkwlrg3',
      'terra12yvwzt5ayh396hgmyd0rwnamgg4g3mxrdgg7c6',
      'terra12897djskt9rge8dtmm86w654g7kzckkd698608'
    ) --bLUNA,bETH,ASTRO,TEST,PSI already decoded
)

SELECT DISTINCT * FROM (
SELECT
  c.block_timestamp,
  c.tx_id,
  c.contract_address,
  coalesce(c.decimals, d.decimals) AS decimals,
  coalesce(c.contract_name, d.contract_name) AS contract_name,
  coalesce(c.symbol, d.symbol) AS symbol,
  c.is_wormhole
FROM
  contract_msgs c
  LEFT JOIN decoded_contracts d ON c.contract_address = d.contract_address
 

UNION ALL

SELECT
  coalesce(c.block_timestamp, m.block_timestamp) AS block_timestamp,
  coalesce(c.tx_id, m.tx_id) AS tx_id,
  c.contract_address,
  decimals,
  contract_name,
  symbol,
  CASE WHEN c.is_wormhole = TRUE THEN TRUE
  ELSE FALSE END AS is_wormhole
FROM 
decoded_contracts c
LEFT JOIN mASSETS m ON c.contract_address = m.contract_address

)
WHERE tx_id IS NOT NULL