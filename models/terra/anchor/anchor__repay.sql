{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'anchor', 'repay', 'address_labels']
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
    AND block_timestamp :: DATE >= (SELECT MAX( block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
  {% endif %}

  GROUP BY 1,
           2,
           3

),

single_payment_tbl_raw AS (
    
  SELECT
    m.blockchain,
    chain_id,
    block_id,
    block_timestamp,
    tx_id,
    msg_value :sender :: STRING AS sender,
    msg_value :coins [0] :amount / pow(10,6) AS amount,
    amount * price AS amount_usd,
    msg_value :coins [0] :denom :: STRING AS currency,
    COALESCE(msg_value :contract :: STRING, '') AS contract_address,
    COALESCE(l.address_name, '') AS contract_label
  FROM {{ ref('silver_terra__msgs') }} m

  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
    ON msg_value :contract :: STRING = l.address 
    AND l.blockchain = 'terra' 
    AND l.creator = 'flipside'
    
  LEFT OUTER JOIN prices r
    ON DATE_TRUNC('hour', block_timestamp) = HOUR
    AND msg_value :coins [0] :denom :: STRING = r.currency
  
  WHERE msg_value :execute_msg :repay_stable IS NOT NULL
    AND tx_status = 'SUCCEEDED'

  {% if is_incremental() %}
    AND block_timestamp :: DATE >= (SELECT MAX( block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
  {% endif %}
    
),

single_payment_tbl_borrower AS (
    
  SELECT 
    tx_id,
    event_type AS from_contract_type,
    MAX(event_attributes:borrower::string) AS borrower
  FROM {{ ref('silver_terra__msg_events') }}
  WHERE event_type = 'from_contract' 

  {% if is_incremental() %}
    AND block_timestamp :: DATE >= (SELECT MAX( block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
  {% endif %}
  
  GROUP BY 1,
           2

),

single_payment_tbl_all AS (
    
  SELECT
    blockchain,
    chain_id,
    block_id,
    block_timestamp,
    a.tx_id,
    sender,
    b.borrower,
    amount,
    amount_usd,
    currency,
    contract_address,
    contract_label,
    from_contract_type
  FROM single_payment_tbl_raw a
  
  LEFT JOIN single_payment_tbl_borrower b
    ON a.tx_id = b.tx_id

),

single_payment_tbl_in_contract AS (
    
  SELECT
    blockchain,
    chain_id,
    block_id,
    block_timestamp,
    tx_id,
    sender,
    borrower,
    amount,
    amount_usd,
    currency,
    contract_address,
    contract_label
  FROM single_payment_tbl_all
  WHERE from_contract_type IS NOT NULL
    
),

single_payment_tbl_out_contract AS (
    
  SELECT
    blockchain,
    chain_id,
    block_id,
    block_timestamp,
    tx_id,
    sender,
    COALESCE(borrower, '') AS borrower,
    amount,
    amount_usd,
    currency,
    contract_address,
    contract_label
  FROM single_payment_tbl_all
  WHERE from_contract_type IS NULL
    
),

single_payment_tbl AS (
    
  SELECT * FROM single_payment_tbl_in_contract
  UNION ALL 
  SELECT * FROM single_payment_tbl_out_contract

),

multiple_repay_tbl_events_raw AS (
    
  SELECT 
    * 
  FROM {{ ref('silver_terra__msg_events') }}
  WHERE
    event_type = 'from_contract'
    AND tx_status = 'SUCCEEDED'
    AND event_attributes :"0_action" :: STRING = 'liquidate_collateral'
    AND event_attributes :"2_repay_amount" IS NOT NULL
    
  {% if is_incremental() %}
    AND block_timestamp :: DATE >= (SELECT MAX( block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
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
    ON a.tx_id = b.tx_id 
    AND a.key_index = b.key_index

),

multiple_repay_borrower_pivot AS (
    
  SELECT 
    tx_id, 
    block_timestamp,
    blockchain,
    chain_id,
    block_id,
    key_index,
    "'liquidator'"::STRING AS sender,
    "'borrower'"::STRING AS borrower,
    "'repay_amount'" AS repay_amount,
    "'stable_denom'"::STRING AS stable_denom,
    "'contract_address'"::STRING AS contract_address
  FROM multiple_repay_borrower_tbl_raw_value
    pivot (max(value) for tx_subtype IN ('liquidator', 'borrower', 'repay_amount', 'stable_denom', 'contract_address')) p

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
    COUNT(DISTINCT key_index) OVER (PARTITION BY tx_id, borrower) AS count_key_index,
    last_value(sender ignore nulls) over (partition by tx_id order by sender) as sender,
    borrower,
    repay_amount AS amount,
    stable_denom AS currency,
    contract_address
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
    sender,
    borrower,
    amount,
    currency,
    contract_address
  FROM multiple_repay_borrower_pivot_agg
  WHERE key_index = max_key_index
  ORDER BY tx_id, 
           block_timestamp,
           blockchain,
           chain_id,
           block_id,
           key_index

),

multiple_payment_tbl AS (
                         
  SELECT
    m.blockchain,
    chain_id,
    block_id,
    block_timestamp,
    tx_id,
    sender,
    borrower,
    amount / pow(10,6) AS amount,
    amount / pow(10,6) * price AS amount_usd,
    COALESCE(m.currency, 'uusd') AS currency,
    contract_address,
    COALESCE(l.address_name, '') AS contract_label
  FROM multiple_repay_borrower_tbl m

  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
    ON m.contract_address = l.address 
    AND l.blockchain = 'terra' 
    AND l.creator = 'flipside'
    
  LEFT OUTER JOIN prices r
    ON DATE_TRUNC('hour', block_timestamp) = HOUR
    AND COALESCE(m.currency, 'uusd') = r.currency

),
liquidate_single AS (

SELECT 
  msg_index,
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  msg_value:sender ::STRING AS sender,
  msg_value:execute_msg:liquidate_collateral:borrower ::STRING AS borrower,
  msg_value:contract ::STRING AS contract_address
FROM terra.msgs 
WHERE msg_value:execute_msg:liquidate_collateral IS NOT NULL
AND tx_status = 'SUCCEEDED'

),

liquidate_single_tbl AS (

SELECT 
  ls.blockchain,
  ls.chain_id,
  ls.block_id,
  ls.block_timestamp,
  ls.tx_id,
  ls.sender,
  ls.borrower,
  event_attributes:"1_repay_amount" / POW(10,6) AS amount,
  event_attributes:"1_repay_amount" / POW(10,6) * r.price AS amount_usd,
  COALESCE(event_attributes:stable_denom ::STRING, 'uusd') as currency,
  ls.contract_address,
  l.address_name AS contract_label
FROM terra.msg_events m

JOIN liquidate_single ls 
  ON m.tx_id = ls.tx_id
  AND m.msg_index = ls.msg_index

LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
    ON ls.contract_address = l.address 
    AND l.blockchain = 'terra' 
    AND l.creator = 'flipside'
    
LEFT OUTER JOIN prices r
  ON DATE_TRUNC('hour', ls.block_timestamp) = HOUR
  AND COALESCE(event_attributes:stable_denom ::STRING, 'uusd') = r.currency

WHERE event_type = 'from_contract'
  AND event_attributes:"2_repay_amount" IS NULL
  AND event_attributes:"0_action" ::STRING = 'liquidate_collateral'

)

SELECT * FROM multiple_payment_tbl
UNION ALL 
SELECT * FROM single_payment_tbl
UNION ALL
SELECT * FROM liquidate_single_tbl