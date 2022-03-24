{{ config(
  materialized = 'incremental',
  unique_key = "contract_address",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'terra_silver', 'contract_metadata', 'terra_contracts']
) }}

WITH contract_events AS (
SELECT
tx_id,
msg_index,
coalesce(event_attributes :contract_address::STRING, event_attributes :"0_contract_address"::STRING) AS contract_address
FROM
{{ ref('silver_terra__msg_events') }}
WHERE
event_type = 'instantiate_contract'

{% if is_incremental() %}
  AND block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
),

contract_msgs AS (
SELECT
tm.tx_id,
contract_address,
msg_value:init_msg:decimals AS decimals,
msg_value:init_msg:name::string AS contract_name,
msg_value:init_msg:symbol::string AS symbol
FROM 
{{ ref('silver_terra__msgs') }} tm
JOIN contract_events ce ON tm.tx_id = ce.tx_id AND tm.msg_index = ce.msg_index
WHERE msg_type = 'wasm/MsgInstantiateContract'

{% if is_incremental() %}
  AND tm.block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
),

wormhole_txs AS (
SELECT
tx_id,
event_attributes :contract_address :: STRING AS token_contract
FROM
  {{ ref("silver_terra__msg_events") }}
WHERE event_attributes :creator = 'terra10nmmwe8r3g99a9newtqa7a75xfgs2e8z87r2sf' --wormhole contracts
AND block_timestamp > '2021-06-01'

),

decoded_contracts AS (
SELECT
tx_id, 
i.address AS contract_address,
init_msg :decimals::NUMERIC AS decimals,
init_msg :name::STRING AS contract_name,
init_msg :symbol::STRING AS symbol
FROM
 {{ ref('silver_terra__contract_info') }} i
LEFT JOIN wormhole_txs w ON i.address = w.token_contract
WHERE contract_address NOT IN ('terra1kc87mu460fwkqte29rquh4hc20m54fxwtsx7gp', 
'terra1dzhzukyezv0etz22ud940z7adyv7xgcjkahuun') --bLUNA and bETH, already decoded
)

SELECT
tx_id,
contract_address,
decimals,
contract_name,
symbol
FROM contract_msgs

UNION ALL

SELECT
tx_id,
contract_address,
decimals,
contract_name,
symbol
FROM decoded_contracts