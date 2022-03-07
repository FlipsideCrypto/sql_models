{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id, msg_index, tx_index)",
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
  WHERE 1=1
  {% if is_incremental() %}
  AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
  {% endif %}

GROUP BY
  1,
  2,
  3
),

source_msgs AS (
  SELECT
    *
  FROM {{ ref('silver_terra__msgs') }}
  WHERE 1=1
  {% if is_incremental() %}
  AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
  {% endif %}
),

source_msg_events AS (
  SELECT
    *
  FROM {{ ref('silver_terra__msg_events') }}
  WHERE 1=1
  {% if is_incremental() %}
  AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
  {% endif %}
),

source_address_labels AS (
  SELECT 
    * 
  FROM {{ ref('silver_crosschain__address_labels') }}
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

---------------
--Single Swap--
---------------

-- Single swap means in ONE msg, there is only ONE swap happened
-- There are 2 types of single swap now: (1) non_array; (2) array. Only difference is how to extract the data
-- For the pool_address, we extracted that from msg, not from events for the single swap
single_msgs_non_array AS (
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
  FROM source_msgs
  WHERE msg_value :execute_msg :swap IS NOT NULL
    AND tx_status = 'SUCCEEDED'

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
  FROM source_msgs
  WHERE msg_value :execute_msg :send :msg :swap IS NOT NULL
    AND tx_status = 'SUCCEEDED'
 
  UNION

  --Undecoded swap messages
  SELECT
    blockchain,
    chain_id,
    block_id,
    msg_index,
    block_timestamp,
    tx_id,
    msg_value :sender :: STRING AS sender,
    msg_value :execute_msg :send :contract :: STRING AS pool_address
  FROM source_msgs
),

-- Single swap within the msg, the txs wrapped in an array
single_msgs_array_raw AS (
  SELECT
    blockchain,
    chain_id,
    block_id,
    msg_index,
    block_timestamp,
    tx_id,
    msg_value :sender :: STRING AS sender,
    msg_value :contract :: STRING AS contract_address
  FROM source_msgs
  WHERE 
    (
      msg_value :execute_msg :execute_swap_operations :operations[0] :native_swap IS NOT NULL
        OR 
      msg_value :execute_msg :execute_swap_operations :operations[0] :terra_swap IS NOT NULL
    )
    AND msg_value :execute_msg :execute_swap_operations :operations[1] :native_swap IS NULL 
    AND msg_value :execute_msg :execute_swap_operations :operations[1] :terra_swap IS NULL
    AND msg_value :execute_msg :execute_swap_operations :operations[1] :astro_swap IS NULL
    AND msg_value :execute_msg :execute_swap_operations :operations[1] :prism_swap IS NULL
    AND msg_value :execute_msg :execute_swap_operations :operations[1] :loop_swap IS NULL
    AND msg_value :execute_msg :execute_swap_operations :operations[2] :native_swap IS NULL 
    AND msg_value :execute_msg :execute_swap_operations :operations[2] :terra_swap IS NULL
    AND msg_value :execute_msg :execute_swap_operations :operations[2] :astro_swap IS NULL
    AND msg_value :execute_msg :execute_swap_operations :operations[2] :prism_swap IS NULL
    AND msg_value :execute_msg :execute_swap_operations :operations[2] :loop_swap IS NULL
    AND tx_status = 'SUCCEEDED'
),

single_msg_array_events AS (
  SELECT 
    source_msg_events.msg_index,
    source_msg_events.tx_id,
    source_msg_events.event_attributes :contract_address :: STRING AS pool_address
  FROM source_msg_events
  INNER JOIN single_msgs_array_raw
  ON source_msg_events.tx_id = single_msgs_array_raw.tx_id
  WHERE event_type = 'from_contract'
), 

single_msgs_array AS (
  SELECT 
    blockchain,
    chain_id,
    block_id,
    single_msgs_array_raw.msg_index,
    block_timestamp,
    single_msgs_array_raw.tx_id,
    sender,
    CASE 
        WHEN single_msg_array_events.pool_address IS NULL 
        THEN single_msgs_array_raw.contract_address
        ELSE single_msg_array_events.pool_address
    END AS pool_address
  FROM single_msgs_array_raw
  LEFT JOIN single_msg_array_events
  ON single_msgs_array_raw.tx_id = single_msg_array_events.tx_id 
    AND single_msgs_array_raw.msg_index = single_msg_array_events.msg_index 
),

msgs AS (
  SELECT
    blockchain,
    chain_id,
    block_id,
    msg_index,
    block_timestamp,
    tx_id,
    sender,
    pool_address
  FROM single_msgs_non_array
  
  UNION ALL 
  
  SELECT 
    blockchain,
    chain_id,
    block_id,
    msg_index,
    block_timestamp,
    tx_id,
    sender,
    pool_address
  FROM single_msgs_array
),

events AS (
  SELECT
    msg_index,
    tx_id,
    event_attributes :offer_amount :: numeric / pow(10,6) AS offer_amount,
    event_attributes :offer_asset :: STRING AS offer_currency,
    event_attributes :return_amount :: numeric / pow(10,6) AS return_amount,
    event_attributes :ask_asset :: STRING AS return_currency
  FROM source_msg_events
  WHERE event_type = 'from_contract'
    AND tx_id IN(SELECT DISTINCT tx_id FROM msgs)
    AND event_attributes :offer_amount IS NOT NULL
), 

swaps AS (
  SELECT
    m.blockchain,
    chain_id,
    block_id,
    m.msg_index,
    0 AS tx_index, --Because there is always only 1 tx index here, so it should be 0
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

  LEFT OUTER JOIN source_address_labels l
    ON pool_address = l.address 
    AND l.blockchain = 'terra' 
    AND l.creator = 'flipside'
),

-----------------
--Multiple Swap--
-----------------
-- First type of multiple swaps, the pool address works for here
msgs_multi_swaps_raw_type_1 AS (
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
  FROM source_msgs
  , lateral flatten ( input => msg_value :execute_msg :run :operations)
  WHERE
    msg_value :execute_msg :run :operations[1] :code ::STRING = 'swap'
    AND tx_status = 'SUCCEEDED'
    AND value :contract :: STRING <> 'uusd'
   
  UNION ALL 
  
  SELECT
    blockchain,
    chain_id,
    block_id,
    msg_index,
    (tx_index - 1) AS tx_index,
    max_tx_index,
    block_timestamp,
    tx_id,
    sender,
    pool_address,
    msg_value
  FROM (
    SELECT
      blockchain,
      chain_id,
      block_id,
      msg_index,
      RANK() OVER (ORDER BY index ASC) AS tx_index,
      MAX(index) OVER (PARTITION BY tx_id) AS max_tx_index,
      block_timestamp,
      tx_id,
      msg_value :sender :: STRING AS sender,
      value :terraswap :ts ::STRING AS pool_address,
      msg_value
    FROM source_msgs
    , lateral flatten ( msg_value :execute_msg :assert_reaction_z :swaps)
    WHERE 
      msg_value :execute_msg :assert_reaction_z :swaps[0] :terraswap IS NOT NULL
      AND tx_status = 'SUCCEEDED'
  )
),

msgs_multi_swaps_raw_type_2_msg AS (
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
    msg_value,
    OBJECT_KEYS(value)[0]::STRING AS tx_type
  FROM source_msgs
  , lateral flatten ( msg_value :execute_msg :execute_swap_operations :operations)
  WHERE msg_value :execute_msg :execute_swap_operations :operations IS NOT NULL
    AND tx_status = 'SUCCEEDED'
    AND OBJECT_KEYS(value)[0]::STRING IN ('terra_swap', 'terraswap', 'native_swap')
  
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
    msg_value, 
    OBJECT_KEYS(value)[0]::STRING AS tx_type
  FROM source_msgs
  , lateral flatten ( msg_value :execute_msg :send :msg :execute_swap_operations :operations)
  WHERE msg_value :execute_msg :send :msg :execute_swap_operations :operations IS NOT NULL
    AND tx_status = 'SUCCEEDED'
    AND OBJECT_KEYS(value)[0]::STRING IN ('terra_swap', 'terraswap', 'native_swap')
),

msgs_multi_swaps_raw_type_2_events_raw AS (
  SELECT 
    tx_id, 
    block_timestamp,
    event_type,
    event_attributes,
    msg_index,
    key,
    value,
    CASE 
        WHEN key REGEXP '^[0-9]' 
        THEN split_part(key, '_', 1) 
        ELSE '0'::STRING 
    END AS tx_index, 
    CASE 
        WHEN key REGEXP '^[0-9]' 
        THEN MAX(split_part(key, '_', 1)) OVER (PARTITION BY tx_id)
        ELSE '0'::STRING 
    END AS max_tx_index, 
    CASE 
        WHEN key REGEXP '^[0-9]' 
        THEN SUBSTRING(key, LEN(split_part(key, '_', 1))+2, LEN(key))
        ELSE key::STRING 
    END AS tx_subtype
  FROM source_msg_events
  , lateral flatten ( input => event_attributes)
  WHERE event_type = 'from_contract'
//    AND tx_id IN (SELECT tx_id FROM msgs_multi_swaps_raw_type_2_msg) 
),

msgs_multi_swaps_raw_type_2_events_index AS (
  SELECT 
    tx_id, 
    block_timestamp,
    msg_index,
    tx_index AS tx_index
  FROM msgs_multi_swaps_raw_type_2_events_raw
  WHERE value = 'swap'
),

msgs_multi_swaps_raw_type_2_events_value AS (
  SELECT 
    a.tx_id, 
    a.msg_index,
    RANK() OVER (PARTITION BY a.tx_id, a.msg_index ORDER BY a.tx_index ASC) AS tx_index,
    a.value::STRING AS pool_address
  FROM msgs_multi_swaps_raw_type_2_events_raw a
  INNER JOIN msgs_multi_swaps_raw_type_2_events_index b
  ON a.tx_id = b.tx_id AND a.tx_index = b.tx_index AND a.msg_index = b.msg_index
  WHERE tx_subtype = 'contract_address'
),

msgs_multi_swaps_raw_type_2 AS (
  SELECT 
    blockchain,
    chain_id,
    block_id,
    a.msg_index,
    a.tx_index,
    max_tx_index,
    block_timestamp,
    a.tx_id,
    sender,
    CASE WHEN b.pool_address IS NULL THEN a.pool_address ELSE b.pool_address END AS pool_address,
    msg_value
  FROM msgs_multi_swaps_raw_type_2_msg a
  LEFT JOIN msgs_multi_swaps_raw_type_2_events_value b
  ON a.tx_id = b.tx_id AND a.msg_index = b.msg_index AND (a.tx_index+1) = b.tx_index
),

msgs_multi_swaps_raw AS (
  SELECT
    blockchain,
    chain_id,
    block_id,
    msg_index,
    tx_index,
    max_tx_index,
    block_timestamp,
    tx_id,
    sender,
    pool_address,
    msg_value
  FROM msgs_multi_swaps_raw_type_1

  UNION ALL 

  SELECT
    blockchain,
    chain_id,
    block_id,
    msg_index,
    tx_index,
    max_tx_index,
    block_timestamp,
    tx_id,
    sender,
    pool_address,
    msg_value
  FROM msgs_multi_swaps_raw_type_2
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

events_multi_swaps_raw_type_1 AS (
  SELECT
    tx_id, 
    event_type,
    event_attributes,
    msg_index,
    event_index,
    tx_index,
    tx_subtype,
    MAX(value) AS value
  FROM (
    SELECT 
        tx_id, 
        event_type,
        event_attributes,
        msg_index,
        event_index,
        value,
        split_part(key, '_', 1) AS tx_index, 
        MAX(split_part(key, '_', 1)) OVER (PARTITION BY tx_id) AS max_tx_index,
        SUBSTRING(key, LEN(split_part(key, '_', 1))+2, LEN(key)) AS tx_subtype
      FROM source_msg_events
      , lateral flatten ( input => event_attributes)
      WHERE event_type = 'from_contract'
      AND event_attributes :"0_offer_amount" IS NOT NULL AND event_attributes :"1_offer_amount" IS NOT NULL
  ) tbl
  WHERE tx_subtype IN ('ask_asset', 'offer_amount', 'offer_asset', 'return_amount', 'contract_address')
  GROUP BY 1,2,3,4,5,6,7
),

events_multi_swaps_type_1 AS (
  SELECT 
    tx_id, 
    msg_index,
    tx_index,
    event_index,
    "'offer_amount'":: numeric / pow(10,6) AS offer_amount,
    "'offer_asset'"::STRING AS offer_currency,
    "'return_amount'":: numeric / pow(10,6) AS return_amount,
    "'ask_asset'"::STRING AS return_currency,
    "'contract_address'"::STRING AS contract_address
  FROM events_multi_swaps_raw_type_1
    pivot (max(value) for tx_subtype IN ('ask_asset', 'offer_amount', 'offer_asset', 'return_amount', 'contract_address')) p
  ORDER BY 
    tx_id, 
    tx_index,
    msg_index,
    event_index
),

events_multi_swaps_raw_type_2 AS (
  SELECT 
    tx_id, 
    event_type,
    event_attributes,
    msg_index,
    event_index,
    t1.value,
    CONCAT(t0.key, '_', t1.key) AS key,
    -1 AS tx_index
  FROM source_msg_events
  , lateral flatten ( input => event_attributes) as t0
  , lateral flatten ( input => t0.value[0] , mode => 'object') t1
  WHERE event_type = 'swap' 
  AND t0.key IN ('offer', 'swap_coin')
),

events_multi_swaps_type_2 AS (
  SELECT 
    tx_id,
    msg_index,
    tx_index,
    event_index,
    offer_amount :: numeric / pow(10,6) AS offer_amount,
    offer_currency,
    return_amount :: numeric / pow(10,6) AS return_amount,
    return_currency,
    contract_address
  FROM (    
    SELECT 
      tx_id, 
      msg_index,
      tx_index,
      event_index,
      "'offer_amount'" AS offer_amount,
      "'offer_denom'"::STRING AS offer_currency,
      "'swap_coin_amount'" AS return_amount,
      "'swap_coin_denom'"::STRING AS return_currency,
      event_attributes :trader ::STRING AS contract_address
    FROM events_multi_swaps_raw_type_2
      pivot (max(value) for key IN ('offer_amount', 'offer_denom', 'swap_coin_amount', 'swap_coin_denom', 'contract_address')) p
    ORDER BY 
      tx_id, 
      tx_index,
      msg_index,
      event_index
  )
),

events_multi_swaps_raw_type_3 AS (
  SELECT
    tx_id, 
    event_type,
    event_attributes,
    msg_index,
    event_index,
    0 AS tx_index,
    key,
    MAX(value) AS value
  FROM (
    SELECT 
        tx_id, 
        event_type,
        event_attributes,
        msg_index,
        event_index,
        key,
        value
      FROM source_msg_events
      , lateral flatten ( input => event_attributes)
      WHERE event_type = 'from_contract'
      AND event_attributes :"offer_amount" IS NOT NULL
  ) tbl
  WHERE key IN ('ask_asset', 'offer_amount', 'offer_asset', 'return_amount', '2_contract_address', 'contract_address')
  GROUP BY 1,2,3,4,5,6,7
),

events_multi_swaps_type_3 AS (
  SELECT 
    tx_id,
    msg_index,
    tx_index,
    event_index,
    offer_amount :: numeric / pow(10,6) AS offer_amount,
    offer_currency,
    return_amount :: numeric / pow(10,6) AS return_amount,
    return_currency,
    contract_address
  FROM (
    SELECT 
      tx_id, 
      msg_index,
      tx_index,
      event_index,
      "'offer_amount'" AS offer_amount,
      "'offer_asset'"::STRING AS offer_currency,
      "'return_amount'" AS return_amount,
      "'ask_asset'"::STRING AS return_currency,
      COALESCE("'2_contract_address'"::STRING, "'contract_address'"::STRING) AS contract_address
    FROM events_multi_swaps_raw_type_3
      pivot (max(value) for key IN ('ask_asset', 'offer_amount', 'offer_asset', 'return_amount', '2_contract_address', 'contract_address')) p
    ORDER BY 
      tx_id, 
      tx_index,
      msg_index,
      event_index
  )
  WHERE offer_amount IS NOT NULL
),

events_multi_swaps AS (
  SELECT 
    tx_id,
    msg_index,
    event_index,
    (tx_index - 1) AS tx_index,
    offer_amount,
    offer_currency,
    return_amount,
    return_currency,
    contract_address
  FROM (
    SELECT 
      tx_id,
      msg_index,
      event_index,
      RANK() OVER(PARTITION BY tx_id, msg_index ORDER BY tx_index ASC)::integer AS tx_index,
      offer_amount,
      offer_currency,
      return_amount,
      return_currency,
      contract_address
    FROM (
      SELECT
        tx_id,
        msg_index,
        tx_index::integer AS tx_index,
        event_index,
        offer_amount,
        offer_currency,
        return_amount,
        return_currency,
        contract_address
      FROM events_multi_swaps_type_1

      UNION ALL 

      SELECT
        tx_id,
        msg_index,
        tx_index::integer AS tx_index,
        event_index,
        offer_amount,
        offer_currency,
        return_amount,
        return_currency,
        contract_address
      FROM events_multi_swaps_type_2

      UNION ALL 

      SELECT
        tx_id,
        msg_index,
        tx_index::integer AS tx_index,
        event_index,
        offer_amount,
        offer_currency,
        return_amount,
        return_currency,
        contract_address
      FROM events_multi_swaps_type_3
    )
  )
  WHERE offer_amount IS NOT NULL
),

swaps_multi_swaps AS (
  SELECT
    a.blockchain,
    chain_id,
    block_id,
    msg_index,
    m_tx_index AS tx_index,
    block_timestamp,
    tx_id,
    sender,
    offer_amount,
    offer_amount_usd,
    offer_currency,
    return_amount,
    return_amount_usd,
    return_currency,
    pool_address,
    l.address_name AS pool_name
  FROM (
    SELECT
      m.blockchain,
      chain_id,
      block_id,
      m.msg_index,
      m.tx_index AS m_tx_index,
      e.tx_index AS e_tx_index,
      block_timestamp,
      m.tx_id,
      sender,
      offer_amount,
      offer_amount * o.price AS offer_amount_usd,
      offer_currency,
      return_amount,
      return_amount * r.price AS return_amount_usd,
      return_currency,
      CASE WHEN m.pool_address IS NOT NULL THEN m.pool_address ELSE e.contract_address END AS pool_address
    FROM
      msgs_multi_swaps m

    LEFT JOIN events_multi_swaps e
      ON m.tx_id = e.tx_id
      AND m.msg_index = e.msg_index

    LEFT OUTER JOIN prices o
      ON DATE_TRUNC('hour',m.block_timestamp) = o.hour
      AND e.offer_currency = o.currency
      
    LEFT OUTER JOIN prices r
      ON DATE_TRUNC('hour',m.block_timestamp) = r.hour
      AND e.return_currency = r.currency
  ) a
  LEFT OUTER JOIN source_address_labels l
    ON a.pool_address = l.address 
    AND l.blockchain = 'terra' 
    AND l.creator = 'flipside'
  WHERE m_tx_index = e_tx_index
)

SELECT DISTINCT
  BLOCKCHAIN,
  CHAIN_ID,
  BLOCK_ID,
  MSG_INDEX,
  TX_INDEX,
  BLOCK_TIMESTAMP,
  TX_ID,
  SENDER,
  OFFER_AMOUNT::FLOAT AS OFFER_AMOUNT,
  OFFER_AMOUNT_USD,
  OFFER_CURRENCY,
  RETURN_AMOUNT,
  RETURN_AMOUNT_USD,
  RETURN_CURRENCY,
  POOL_ADDRESS,
  POOL_NAME
FROM swaps
WHERE OFFER_CURRENCY NOT LIKE 'cw20:terra%' -- Remove PRISM contract
    AND RETURN_CURRENCY NOT LIKE 'cw20:terra%' -- Remove PRISM contract
    AND POOL_ADDRESS NOT IN (SELECT contract_address from astro_pairs) --Remove astroport swaps

UNION ALL 

SELECT DISTINCT
  BLOCKCHAIN,
  CHAIN_ID,
  BLOCK_ID,
  MSG_INDEX,
  TX_INDEX,
  BLOCK_TIMESTAMP,
  TX_ID,
  SENDER,
  OFFER_AMOUNT::FLOAT AS OFFER_AMOUNT,
  OFFER_AMOUNT_USD,
  OFFER_CURRENCY,
  RETURN_AMOUNT,
  RETURN_AMOUNT_USD,
  RETURN_CURRENCY,
  POOL_ADDRESS,
  POOL_NAME
FROM swaps_multi_swaps
WHERE OFFER_CURRENCY NOT LIKE 'cw20:terra%' -- Remove PRISM contract
    AND RETURN_CURRENCY NOT LIKE 'cw20:terra%' -- Remove PRISM contract
    AND POOL_ADDRESS NOT IN (SELECT contract_address from astro_pairs) --remove astroport swaps