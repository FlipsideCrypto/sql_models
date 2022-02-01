{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', tx_id, msg_index, action_index, denom, amount)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra_silver', 'terra_transfer_events']
) }}

WITH silver_terra_raw AS (
  SELECT
    *
  FROM
  {{ ref('terra_dbt__msg_events') }}
  WHERE
  1 = 1

  {% if is_incremental() %}
  AND system_created_at :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(system_created_at :: DATE))
  FROM
    {{ this }} AS msg_events
)
{% endif %}

  QUALIFY(ROW_NUMBER() OVER(PARTITION BY chain_id, block_id, tx_id, msg_index, event_index, event_type
  ORDER BY system_created_at DESC)) = 1
),

silver_terra_raw_lateral AS (
  SELECT 
    system_created_at,
    block_id,
    block_timestamp,
    blockchain,
    chain_id,
    tx_id,
    tx_status,
    tx_module,
    tx_type,
    msg_index,
    msg_module,
    msg_type,
    event_index,
    event_type,
    event_attributes,
    event_attributes_array_index,  
    object_insert(
      object_agg(
        event_attributes_array_key, event_attributes_array_values
      ),
      'array_index', event_attributes_array_index
    ) AS event_attributes_array
  FROM (
    SELECT
      system_created_at,
      block_id,
      block_timestamp,
      blockchain,
      chain_id,
      tx_id,
      tx_status,
      tx_module,
      tx_type,
      msg_index,
      msg_module,
      msg_type,
      event_index,
      event_type,
      event_attributes,
      CASE 
        WHEN try_to_decimal(TO_CHAR(split(f.key, '_' )[0])) IS NOT NULL 
        THEN TO_CHAR(split(f.key, '_' )[0])
        ELSE '0'
      END AS event_attributes_array_index,
      CASE 
        WHEN try_to_decimal(TO_CHAR(split(f.key, '_' )[0])) IS NOT NULL 
        THEN array_to_string(array_slice(split(f.key, '_' ), 1, array_size(split(f.key, '_' ))), '_') 
        ELSE array_to_string(array_slice(split(f.key, '_' ), 0, array_size(split(f.key, '_' ))), '_')
      END AS event_attributes_array_key,
      f.value AS event_attributes_array_values
    FROM silver_terra_raw,
    lateral flatten(input => event_attributes) AS f
  )
  GROUP BY 
    system_created_at,
    block_id,
    block_timestamp,
    blockchain,
    chain_id,
    tx_id,
    tx_status,
    tx_module,
    tx_type,
    msg_index,
    msg_module,
    msg_type,
    event_index,
    event_type,
    event_attributes,
    event_attributes_array_index
),

silver_terra_arr AS (
SELECT 
  system_created_at,
  block_id,
  block_timestamp,
  blockchain,
  chain_id,
  tx_id,
  tx_status,
  tx_module,
  tx_type,
  msg_index,
  msg_module,
  msg_type,
  event_index,
  event_type,
  event_attributes,
  array_agg(event_attributes_array) WITHIN group (ORDER BY event_attributes_array_index ASC) AS event_attributes_array
FROM silver_terra_raw_lateral
GROUP BY 
  system_created_at,
  block_id,
  block_timestamp,
  blockchain,
  chain_id,
  tx_id,
  tx_status,
  tx_module,
  tx_type,
  msg_index,
  msg_module,
  msg_type,
  event_index,
  event_type,
  event_attributes
),

all_xfers AS ( 

--Event_type transfers
SELECT
    block_timestamp,
    tx_id,
    msg_index,
    event_type,
    a.value:array_index::STRING AS array_index,
    a.value:sender::STRING AS sender,
    a.value:recipient::STRING AS recipient,
    a.value:amount[0]:denom::STRING AS denom,
    a.value:amount[0]:amount / POW(10,6) AS amount
    FROM silver_terra_arr r,
    lateral flatten(input => r.event_attributes_array) a
    WHERE event_type = 'transfer'
    AND tx_status = 'SUCCEEDED'
    AND a.value:amount[0]:amount IS NOT NULL
    AND msg_type <> 'distribution/MsgWithdrawDelegatorReward'

UNION 

--Unnested delegator rewards transfers
SELECT
    block_timestamp,
    tx_id,
    msg_index,
    event_type,
    event_attributes_array[0]:array_index::STRING AS array_index,
    event_attributes_array[0]:sender::STRING AS sender,
    event_attributes_array[0]:recipient::STRING AS recipient,
    a.value:denom::STRING AS denom,
    a.value:amount / POW(10,6) AS amount
    FROM silver_terra_arr r,
    lateral flatten(input => r.event_attributes_array[0]:amount) a
    WHERE event_type = 'transfer'
    AND msg_type = 'distribution/MsgWithdrawDelegatorReward' 
    AND tx_status = 'SUCCEEDED'

UNION
  
--Wormhole transfers
SELECT
    block_timestamp,
    tx_id,
    msg_index,
    event_type,
    a.value:array_index::STRING AS array_index,
    a.value:sender::STRING AS sender,
    a.value:recipient::STRING AS recipient,
    a.value:contract::STRING AS denom,
    a.value:amount / POW(10,6) AS amount
    FROM silver_terra_arr r,
    lateral flatten(input => r.event_attributes_array) a
    WHERE r.event_type = 'from_contract' 
    AND a.value:action::string = 'complete_transfer_wrapped'
    AND r.event_attributes:"0_contract_address"::string = 'terra10nmmwe8r3g99a9newtqa7a75xfgs2e8z87r2sf'
    AND tx_status = 'SUCCEEDED'

UNION

--cw20 nested transfers/sends
SELECT
    block_timestamp,
    tx_id,
    msg_index,
    event_type,
    a.value:array_index::STRING AS array_index,
    a.value:from::STRING AS sender,
    a.value:to::STRING AS recipient,
    a.value:contract_address::STRING AS denom,
    a.value:amount / POW(10,6) as amount
    FROM silver_terra_arr r,
    lateral flatten(input => r.event_attributes_array) a
    WHERE event_type = 'from_contract'
    AND array_index = '0'
    AND a.value:action::STRING IN ('send', 'transfer')
    AND tx_status = 'SUCCEEDED'
    AND a.value:amount IS NOT NULL
),

msgs AS (
    SELECT 
    tx_id,
    r.msg_index,
    a.value:array_index::STRING AS array_index,
    a.value:sender::STRING AS sender,
    a.value:contract::STRING AS contract_address
    FROM
    silver_terra_arr r,
    lateral flatten(input => r.event_attributes_array) a
    WHERE tx_id IN (SELECT tx_id from all_xfers)
    AND r.event_type = 'message'
    GROUP BY 
    tx_id,
    r.msg_index,
    array_index,
    sender,
    contract_address

),

msg_actions AS (
  SELECT 
  tx_id,
  msg_type,
  msg_value,
  tx_status,
  msg_index,
  OBJECT_KEYS(msg_value:execute_msg)[0]::STRING AS execute_msg_type,
  msg_value:contract::STRING AS contract,
  REGEXP_SUBSTR(msg_type,'[A-z]*$') AS parsed_type,
  COALESCE(execute_msg_type,parsed_type) AS label
  FROM {{ ref('silver_terra__msgs') }}
  WHERE tx_id IN (SELECT tx_id FROM all_xfers)
  AND tx_status ='SUCCEEDED'

  {% if is_incremental() %}
  AND block_timestamp >= getdate() - INTERVAL '1 days'
  {% endif %}
  )
      

   SELECT 
    x.block_timestamp,
    x.tx_id,
    x.msg_index,
    x.array_index AS action_index,
    i.label AS initial_action,
    c.label AS current_action,
    COALESCE(x.sender,m.sender) AS sender,
    x.recipient,
    x.denom,
    x.amount,
    m.sender AS msg_sender,
    COALESCE(m.contract_address,c.contract) AS contract_address
    FROM
    all_xfers x
    LEFT OUTER JOIN
    msgs m
        ON x.tx_id = m.tx_id AND x.array_index = m.array_index AND x.msg_index = m.msg_index
    LEFT OUTER JOIN
    msg_actions i
        ON x.tx_id = i.tx_id AND i.msg_index = 0
    LEFT OUTER JOIN
    msg_actions c
        ON x.tx_id = c.tx_id AND x.msg_index = c.msg_index
        ORDER BY block_timestamp DESC, tx_id