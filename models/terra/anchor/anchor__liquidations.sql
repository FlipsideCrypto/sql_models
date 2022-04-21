{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'anchor', 'liquidations', 'address_labels']
) }}

WITH prices AS (

  SELECT
    DATE_TRUNC('hour', block_timestamp) AS HOUR,
    currency,
    symbol,
    AVG(price_usd) AS price
  FROM {{ ref('terra__oracle_prices') }}
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
    
  SELECT
    m.blockchain,
    chain_id,
    block_id,
    block_timestamp,
    tx_id,
    msg_value :execute_msg :liquidate_collateral :borrower :: STRING AS borrower,
    msg_value :contract :: STRING AS contract_address,
    l.address_name AS contract_label
  FROM {{ ref('silver_terra__msgs') }} m

LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
  ON msg_value :contract :: STRING = l.address 
  AND l.blockchain = 'terra' 
  AND l.creator = 'flipside'

WHERE msg_value :contract :: STRING = 'terra1tmnqgvg567ypvsvk6rwsga3srp7e3lg6u0elp8'
  AND msg_value :execute_msg :liquidate_collateral IS NOT NULL
  AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
{% endif %}
    
),

events_first_token AS (
    
  SELECT
    tx_id,
    COALESCE(event_attributes :liquidator :: STRING, event_attributes :"0_liquidator" :: STRING) AS liquidator,
    COALESCE(event_attributes :collateral_amount / pow(10,6), event_attributes :"0_collateral_amount" / pow(10,6)) AS liquidated_amount,
    liquidated_amount * l.price AS liquidated_amount_usd,
    COALESCE(event_attributes :collateral_token :: STRING, event_attributes :"0_collateral_token" :: STRING) AS liquidated_currency,
    CASE 
      WHEN event_attributes :"2_repay_amount" IS NULL THEN event_attributes :"1_repay_amount" / pow(10,6) 
      ELSE event_attributes :"0_repay_amount" / pow(10,6)
    END AS repay_amount,
    event_attributes :"1_borrower" :: STRING AS borrower,
    repay_amount * r.price AS repay_amount_usd,
    COALESCE(event_attributes :stable_denom :: STRING, event_attributes :"0_stable_denom" :: STRING) AS repay_currency,
    COALESCE(event_attributes :bid_fee / pow(10,6), event_attributes :"0_bid_fee" / pow(10,6)) AS bid_fee
  FROM {{ ref('silver_terra__msg_events') }}
    
LEFT OUTER JOIN prices l
  ON DATE_TRUNC('hour', block_timestamp) = l.hour
  AND COALESCE(event_attributes :collateral_token :: STRING, event_attributes :"0_collateral_token" :: STRING) = l.currency
    
LEFT OUTER JOIN prices r
  ON DATE_TRUNC('hour', block_timestamp) = r.hour
  AND COALESCE(event_attributes :stable_denom :: STRING, event_attributes :"0_stable_denom" :: STRING) = r.currency
  
WHERE event_type = 'from_contract'
  AND tx_id IN(SELECT tx_id FROM msgs)
  AND tx_status = 'SUCCEEDED'
  AND event_attributes :"0_action" :: STRING = 'liquidate_collateral'

{% if is_incremental() %}
AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
{% endif %}
    
),
events_second_tokens AS (
  SELECT
    tx_id,
    COALESCE(event_attributes :liquidator :: STRING, event_attributes :"1_liquidator" :: STRING) AS liquidator,
    COALESCE(event_attributes :collateral_amount / pow(10,6), event_attributes :"1_collateral_amount" / pow(10,6)) AS liquidated_amount,
    liquidated_amount * l.price AS liquidated_amount_usd,
    COALESCE(event_attributes :collateral_token :: STRING, event_attributes :"1_collateral_token" :: STRING) AS liquidated_currency,
    event_attributes :"1_repay_amount" / pow(10,6) AS repay_amount,
    event_attributes :"1_borrower" :: STRING AS borrower,
    repay_amount * r.price AS repay_amount_usd,
    COALESCE(event_attributes :stable_denom :: STRING, event_attributes :"1_stable_denom" :: STRING) AS repay_currency,
    COALESCE(event_attributes :bid_fee / pow(10,6), event_attributes :"1_bid_fee" / pow(10,6)) AS bid_fee
  FROM {{ ref('silver_terra__msg_events') }}
    
LEFT OUTER JOIN prices l
  ON DATE_TRUNC('hour', block_timestamp) = l.hour
  AND COALESCE(event_attributes :collateral_token :: STRING, event_attributes :"1_collateral_token" :: STRING) = l.currency
    
LEFT OUTER JOIN prices r
  ON DATE_TRUNC('hour', block_timestamp) = r.hour
  AND COALESCE(event_attributes :stable_denom :: STRING, event_attributes :"1_stable_denom" :: STRING) = r.currency
  
WHERE event_type = 'from_contract'
  AND tx_id IN( SELECT tx_id FROM msgs)
  AND tx_status = 'SUCCEEDED'
  AND event_attributes :"0_action" :: STRING = 'liquidate_collateral'
  AND event_attributes :"2_repay_amount" IS NOT NULL 

{% if is_incremental() %}
AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
{% endif %}
    
),

events AS (
    
  SELECT * FROM events_first_token
  UNION ALL 
  SELECT * FROM events_second_tokens
    
),

single_payment_tbl AS (
    
  SELECT DISTINCT
    blockchain,
    chain_id,
    block_id,
    block_timestamp,
    m.tx_id,
    bid_fee,
    m.borrower,
    liquidator,
    liquidated_amount,
    liquidated_amount_usd,
    liquidated_currency,
    -- repay_amount,
    -- repay_amount_usd,
    -- repay_currency,
    contract_address,
    contract_label
  FROM msgs m

  JOIN events e
    ON m.tx_id = e.tx_id
    AND m.borrower = e.borrower 

),

multiple_repay_tbl_events_raw AS (
    
  SELECT 
    * 
  FROM {{ ref('silver_terra__msg_events') }}
  WHERE event_type = 'from_contract'
    AND tx_status = 'SUCCEEDED'
    AND event_attributes :"0_action" :: STRING = 'liquidate_collateral'
    AND event_attributes :"3_repay_amount" IS NOT NULL

  {% if is_incremental() %}
    AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
  {% endif %}

),

multiple_repay_tbl_raw AS (
    
  SELECT 
    *,
    SPLIT_PART(key, '_', 0) AS key_index
  FROM multiple_repay_tbl_events_raw
  , lateral flatten ( input => event_attributes )

),

multiple_repay_borrower_tbl_raw_index AS (
    
  SELECT 
    tx_id,
    SPLIT_PART(key, '_', 0) AS key_index
  FROM multiple_repay_tbl_raw
  WHERE key LIKE '%_borrower'

),

multiple_repay_borrower_tbl_raw_value AS (
    
  SELECT 
    a.blockchain,
    a.chain_id,
    a.block_id,
    a.block_timestamp,
    a.tx_id,
    a.key_index,
    -- key, 
    SUBSTRING(key, LEN(split_part(key, '_', 1))+2, LEN(key)) AS tx_subtype,
    value
  FROM multiple_repay_tbl_raw a

  INNER JOIN multiple_repay_borrower_tbl_raw_index b
    ON a.tx_id = b.tx_id AND a.key_index = b.key_index

),

multiple_repay_borrower_pivot AS (
    
  SELECT 
    tx_id, 
    block_timestamp,
    blockchain,
    chain_id,
    block_id,
    key_index,
    "'borrower'"::STRING AS borrower,
    "'repay_amount'" AS repay_amount
  FROM multiple_repay_borrower_tbl_raw_value
    pivot (max(value) for tx_subtype IN ('borrower', 'repay_amount')) p

),

multiple_repay_borrower_pivot_agg AS (
    
  SELECT
    tx_id, 
    block_timestamp,
    blockchain,
    chain_id,
    block_id,
    key_index,
    MAX(key_index) OVER (PARTITION BY tx_id, borrower) AS max_key_index,
    MIN(key_index) OVER (PARTITION BY tx_id, borrower) AS min_key_index,
    COUNT(DISTINCT key_index) OVER (PARTITION BY tx_id, borrower) AS count_key_index,
    borrower,
    repay_amount
  FROM multiple_repay_borrower_pivot

),

multiple_repay_borrower_tbl AS (
    
  SELECT 
    tx_id, 
    block_timestamp,
    blockchain,
    chain_id,
    block_id,
    key_index,
    RANK() OVER (PARTITION BY tx_id ORDER BY key_index ASC) AS key_index_rank,
    SUM(count_key_index-2) OVER (PARTITION BY tx_id ORDER BY key_index ASC) AS count_key_index,
    borrower,
    repay_amount
  FROM multiple_repay_borrower_pivot_agg
  WHERE key_index <> min_key_index
  ORDER BY 
    tx_id, 
    block_timestamp,
    blockchain,
    chain_id,
    block_id,
    key_index

),

multiple_repay_collateral_tbl_raw_index AS (
    
  SELECT 
    tx_id,
    SPLIT_PART(key, '_', 0) AS key_index
  FROM multiple_repay_tbl_raw
  WHERE key LIKE '%_collateral_amount'

),

multiple_repay_collateral_tbl_raw_value AS (
    
  SELECT 
    a.blockchain,
    a.chain_id,
    a.block_id,
    a.block_timestamp,
    a.tx_id,
    a.key_index,
    -- key, 
    SUBSTRING(key, LEN(split_part(key, '_', 1))+2, LEN(key)) AS tx_subtype,
    value
  FROM multiple_repay_tbl_raw a
  
  INNER JOIN multiple_repay_collateral_tbl_raw_index b
    ON a.tx_id = b.tx_id 
    AND a.key_index = b.key_index

),

multiple_repay_collateral_tbl AS (
    
  SELECT 
    tx_id, 
    block_timestamp,
    blockchain,
    chain_id,
    block_id,
    key_index,
    "'stable_denom'"::STRING AS stable_denom,
    "'liquidator_fee'" AS liquidator_fee,
    "'liquidator'"::STRING AS liquidator,
    "'collateral_token'"::STRING AS collateral_token,
    "'collateral_amount'" AS collateral_amount,
    "'bid_fee'" AS bid_fee
  FROM multiple_repay_collateral_tbl_raw_value
    pivot (max(value) for tx_subtype IN ('stable_denom', 'liquidator_fee', 'liquidator', 'collateral_token', 'collateral_amount', 'bid_fee')) p

),

multiple_repay_tbl_borrower_collateral AS (
    
  SELECT 
    a.blockchain,
    a.chain_id,
    a.block_id,
    a.block_timestamp,
    a.tx_id, 
    b.bid_fee,
    a.borrower,
    b.liquidator,
    b.collateral_amount,
    b.collateral_token,
    a.repay_amount,
    b.stable_denom
  FROM multiple_repay_borrower_tbl a
  LEFT JOIN multiple_repay_collateral_tbl b
  ON a.tx_id = b.tx_id AND (a.key_index_rank-1) = b.key_index
    
),

multiple_repay_tbl AS (
    
  SELECT 
    blockchain,
    chain_id,
    block_id,
    block_timestamp,
    tx_id,
    bid_fee / pow(10,6) AS bid_fee,
    borrower AS borrower,
    liquidator AS liquidator,
    collateral_amount / pow(10,6) AS liquidated_amount,
    liquidated_amount * l.price AS liquidated_amount_usd,
    collateral_token AS liquidated_currency,
    -- repay_amount / pow(10,6) AS repay_amount,
    -- repay_amount / pow(10,6) * r.price AS repay_amount_usd,
    -- stable_denom::STRING AS repay_currency,
    'terra1tmnqgvg567ypvsvk6rwsga3srp7e3lg6u0elp8' AS contract_address,
    'Overseer' AS contract_label
  FROM multiple_repay_tbl_borrower_collateral

  LEFT OUTER JOIN prices l
    ON DATE_TRUNC('hour',block_timestamp) = l.hour
    AND collateral_token = l.currency
  
  LEFT OUTER JOIN prices r
    ON DATE_TRUNC('hour',block_timestamp) = r.hour
    AND stable_denom = r.currency
),

premium_rate AS (

SELECT 
  tx_id,
  msg_value:execute_msg:submit_bid:collateral_token ::STRING AS collateral_token,
  msg_value:execute_msg:submit_bid:premium_rate AS premium_rate
FROM {{ ref('silver_terra__msgs') }}
WHERE msg_value:execute_msg:submit_bid:premium_rate IS NOT NULL
AND tx_status = 'SUCCEEDED'

)

SELECT DISTINCT * FROM (
SELECT
  BLOCKCHAIN,
  CHAIN_ID::STRING AS CHAIN_ID,
  BLOCK_ID,
  BLOCK_TIMESTAMP,
  st.TX_ID,
  BID_FEE,
  p.premium_rate,
  BORROWER,
  LIQUIDATOR,
  LIQUIDATED_AMOUNT,
  LIQUIDATED_AMOUNT_USD,
  LIQUIDATED_CURRENCY,
  CONTRACT_ADDRESS,
  CONTRACT_LABEL
FROM single_payment_tbl st

LEFT OUTER JOIN premium_rate p
  ON st.tx_id = p.tx_id
  AND st.liquidated_currency = p.collateral_token

UNION ALL 

SELECT
  BLOCKCHAIN,
  CHAIN_ID::STRING AS CHAIN_ID,
  BLOCK_ID,
  BLOCK_TIMESTAMP,
  mt.TX_ID,
  BID_FEE,
  pr.premium_rate,
  BORROWER,
  LIQUIDATOR,
  LIQUIDATED_AMOUNT,
  LIQUIDATED_AMOUNT_USD,
  LIQUIDATED_CURRENCY,
  CONTRACT_ADDRESS,
  CONTRACT_LABEL
FROM multiple_repay_tbl mt

LEFT OUTER JOIN premium_rate pr
  ON mt.tx_id = pr.tx_id
  AND mt.liquidated_currency = pr.collateral_token
)