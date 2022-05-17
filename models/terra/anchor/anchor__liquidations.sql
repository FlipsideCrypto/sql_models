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

liquidations AS (
SELECT
blockchain,
chain_id,
block_id,
block_timestamp,
tx_id,
action_index,
msg_index,
action_log :borrower::STRING AS borrower,
action_log :liquidator::STRING AS liquidator,
action_log :amount / POW(10,6) AS liquidated_amount,
action_contract_address AS contract_address
FROM {{ ref('silver_terra__event_actions') }}
WHERE action_method = 'liquidate_collateral'

{% if is_incremental() %}
AND
  block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
),

bid_fees AS (
SELECT
  tx_id,
  msg_index,
  action_log :bid_fee / POW(10,6) AS bid_fee,
  action_log :collateral_amount / POW(10,6) AS amount,
  action_log :collateral_token::STRING AS liquidated_currency
FROM {{ ref('silver_terra__event_actions') }}
WHERE action_method = 'execute_bid'

{% if is_incremental() %}
AND
  block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
  ),

premium_rate AS (

SELECT 
  tx_id,
  msg_value:execute_msg:submit_bid:collateral_token ::STRING AS collateral_token,
  msg_value:execute_msg:submit_bid:premium_rate AS premium_rate
FROM {{ ref('silver_terra__msgs') }}
WHERE msg_value:execute_msg:submit_bid:premium_rate IS NOT NULL
AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
AND
  block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
)

SELECT DISTINCT * FROM (
SELECT
  l.blockchain,
  l.chain_id,
  l.block_id,
  l.block_timestamp,
  l.tx_id,
  bid_fee,
  premium_rate,
  borrower,
  liquidator,
  liquidated_amount,
  liquidated_amount * price AS liquidated_amount_usd,
  liquidated_currency,
  contract_address,
  COALESCE(la.address_name, '') AS contract_label
 FROM liquidations l
 
 LEFT JOIN bid_fees b
 ON l.tx_id = b.tx_id
 AND l.liquidated_amount = b.amount
 AND l.msg_index = b.msg_index
 
 LEFT JOIN premium_rate p
 ON b.tx_id = p.tx_id
 AND b.liquidated_currency = p.collateral_token

 LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS la
  ON l.contract_address = la.address AND la.blockchain = 'terra' AND la.creator = 'flipside'

LEFT OUTER JOIN prices o
ON DATE_TRUNC(
    'hour',
    l.block_timestamp
  ) = o.hour
  AND b.liquidated_currency = o.currency
)