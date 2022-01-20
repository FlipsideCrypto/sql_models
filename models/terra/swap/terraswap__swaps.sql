{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'terraswap', 'swap', 'address_labels']
) }}

WITH prices AS (

  SELECT
    DATE_TRUNC('hour',block_timestamp) AS HOUR,
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

msgs AS (
  -- native to non-native/native
  SELECT
    blockchain,
    chain_id,
    block_id,
    msg_index,
    block_timestamp,
    tx_id,
    msg_value :sender :: STRING AS sender,
    msg_value :contract :: STRING AS pool_address
  FROM {{ ref('silver_terra__msgs') }}
  WHERE msg_value :execute_msg :swap IS NOT NULL
    AND tx_status = 'SUCCEEDED'

  {% if is_incremental() %}
    AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
  {% endif %}

  UNION

  -- non-native to native
  SELECT
    blockchain,
    chain_id,
    block_id,
    msg_index,
    block_timestamp,
    tx_id,
    msg_value :sender :: STRING AS sender,
    msg_value :execute_msg :send :contract :: STRING AS pool_address
  FROM {{ ref('silver_terra__msgs') }}
  WHERE msg_value :execute_msg :send :msg :swap IS NOT NULL
    AND tx_status = 'SUCCEEDED'

  {% if is_incremental() %}
    AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
  {% endif %}
),

events AS (
  SELECT
    msg_index,
    tx_id,
    event_attributes :offer_amount :: numeric / pow(10,6) AS offer_amount,
    event_attributes :offer_asset :: STRING AS offer_currency,
    event_attributes :return_amount :: numeric / pow(10,6) AS return_amount,
    event_attributes :ask_asset :: STRING AS return_currency
  FROM {{ ref('silver_terra__msg_events') }}
  WHERE event_type = 'from_contract'
    AND tx_id IN(SELECT DISTINCT tx_id FROM msgs)
    AND event_attributes :offer_amount IS NOT NULL

  {% if is_incremental() %}
    AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
  {% endif %}
), 

swaps AS (
  SELECT
    m.blockchain,
    chain_id,
    block_id,
    m.msg_index,
    0 AS tx_index,
    block_timestamp,
    m.tx_id,
    sender,
    offer_amount,
    offer_amount * o.price AS offer_amount_usd,
    offer_currency,
    return_amount,
    return_amount * r.price AS return_amount_usd,
    return_currency,
    pool_address,
    l.address_name AS pool_name
  FROM
    msgs m

  JOIN events e
    ON m.tx_id = e.tx_id
    AND m.msg_index = e.msg_index

  LEFT OUTER JOIN prices o
    ON DATE_TRUNC('hour',m.block_timestamp) = o.hour
    AND e.offer_currency = o.currency
    
  LEFT OUTER JOIN prices r
    ON DATE_TRUNC('hour',m.block_timestamp) = r.hour
    AND e.return_currency = r.currency

  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} l
    ON pool_address = l.address 
    AND l.blockchain = 'terra' 
    AND l.creator = 'flipside'
),

msgs_multi_swaps_raw AS (
  SELECT
    blockchain,
    chain_id,
    block_id,
    msg_index,
    index AS tx_index,
    MAX(index) OVER (PARTITION BY tx_id) AS max_tx_index,
    block_timestamp,
    tx_id,
    msg_value :sender :: STRING AS sender,
    value :contract :: STRING AS pool_address,
    msg_value
  FROM {{ ref('silver_terra__msgs') }}
  , lateral flatten ( input => msg_value :execute_msg :run :operations)
  WHERE
    msg_value :execute_msg :run :operations[0] :code ::STRING = 'swap'
    AND tx_status = 'SUCCEEDED'
  
  UNION ALL 

  SELECT
    blockchain,
    chain_id,
    block_id,
    msg_index,
    index AS tx_index,
    MAX(index) OVER (PARTITION BY tx_id) AS max_tx_index,
    block_timestamp,
    tx_id,
    msg_value :sender :: STRING AS sender,
    msg_value :contract :: STRING AS pool_address,
    msg_value
  FROM {{ ref('silver_terra__msgs') }}
  , lateral flatten ( msg_value :execute_msg :execute_swap_operations :operations)
  WHERE 
    msg_value :execute_msg :execute_swap_operations :operations[0] :terra_swap IS NOT NULL
    AND tx_status = 'SUCCEEDED'
  
  UNION ALL 

  SELECT
    blockchain,
    chain_id,
    block_id,
    msg_index,
    index AS tx_index,
    MAX(index) OVER (PARTITION BY tx_id) AS max_tx_index,
    block_timestamp,
    tx_id,
    msg_value :sender :: STRING AS sender,
    msg_value :execute_msg :send :contract :: STRING AS pool_address,
    msg_value
  FROM {{ ref('silver_terra__msgs') }}
  , lateral flatten ( msg_value :execute_msg :send :msg :execute_swap_operations :operations)
  WHERE
    msg_value :execute_msg :send :msg :execute_swap_operations :operations[0] :terra_swap IS NOT NULL
    AND tx_status = 'SUCCEEDED'
),

msgs_multi_swaps AS (
  SELECT 
    blockchain,
    chain_id,
    block_id,
    msg_index,
    tx_index,
    block_timestamp,
    tx_id,
    sender,
    pool_address,
    msg_value
  FROM msgs_multi_swaps_raw
  WHERE max_tx_index > 0
),

events_multi_swaps_raw AS (
  SELECT
    tx_id, 
    event_type,
    event_attributes,
    msg_index,
    tx_index,
    tx_subtype,
    MAX(value) AS value
  FROM (
    SELECT 
        tx_id, 
        event_type,
        event_attributes,
        msg_index,
        value,
        split_part(key, '_', 1) AS tx_index, 
        MAX(split_part(key, '_', 1)) OVER (PARTITION BY tx_id) AS max_tx_index,
        SUBSTRING(key, LEN(split_part(key, '_', 1))+2, LEN(key)) AS tx_subtype
      FROM {{ ref('silver_terra__msg_events') }}
      , lateral flatten ( input => event_attributes)
      WHERE event_type = 'from_contract'
      AND event_attributes :"0_offer_amount" IS NOT NULL AND event_attributes :"1_offer_amount" IS NOT NULL
  ) tbl
  WHERE tx_subtype IN ('ask_asset', 'offer_amount', 'offer_asset', 'return_amount')
  GROUP BY 1,2,3,4,5,6
),

events_multi_swaps AS (
  SELECT 
    tx_id,
    msg_index,
    tx_index,
    offer_amount :: numeric / pow(10,6) AS offer_amount,
    offer_currency,
    return_amount :: numeric / pow(10,6) AS return_amount,
    return_currency
  FROM (
    SELECT 
      tx_id, 
      msg_index,
      tx_index,
      "'offer_amount'" AS offer_amount,
      "'offer_asset'"::STRING AS offer_currency,
      "'return_amount'" AS return_amount,
      "'ask_asset'"::STRING AS return_currency
    FROM events_multi_swaps_raw
      pivot (max(value) for tx_subtype IN ('ask_asset', 'offer_amount', 'offer_asset', 'return_amount')) p
    ORDER BY 
      tx_id, 
      tx_index,
      msg_index
  )
  WHERE offer_amount IS NOT NULL
),

swaps_multi_swaps AS (
  SELECT
    m.blockchain,
    chain_id,
    block_id,
    m.msg_index,
    m.tx_index,
    block_timestamp,
    m.tx_id,
    sender,
    offer_amount,
    offer_amount * o.price AS offer_amount_usd,
    offer_currency,
    return_amount,
    return_amount * r.price AS return_amount_usd,
    return_currency,
    pool_address,
    l.address_name AS pool_name
  FROM
    msgs_multi_swaps m

  LEFT JOIN events_multi_swaps e
    ON m.tx_id = e.tx_id
    AND m.msg_index = e.msg_index
    AND m.tx_index::STRING = e.tx_index::STRING

  LEFT OUTER JOIN prices o
    ON DATE_TRUNC('hour',m.block_timestamp) = o.hour
    AND e.offer_currency = o.currency
    
  LEFT OUTER JOIN prices r
    ON DATE_TRUNC('hour',m.block_timestamp) = r.hour
    AND e.return_currency = r.currency

  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} l
    ON pool_address = l.address 
    AND l.blockchain = 'terra' 
    AND l.creator = 'flipside'
)

SELECT DISTINCT * FROM swaps
UNION ALL 
SELECT DISTINCT * FROM swaps_multi_swaps
