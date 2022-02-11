{{ config(
  materialized = 'view',
  tags = ['snowflake', 'terra_views', 'transfer_events', 'terra']
) }}

WITH prices AS (

SELECT
    DATE_TRUNC('hour', block_timestamp) AS HOUR,
    currency,
    symbol,
    AVG(price_usd) AS price_usd
  FROM {{ ref('terra__oracle_prices') }}
  GROUP BY
    1,
    2,
    3

),

symbol AS (

SELECT
  currency,
  symbol
FROM {{ ref('terra__oracle_prices') }}
WHERE block_timestamp >= CURRENT_DATE - 2
GROUP BY 1,
         2

)

SELECT
  t.block_timestamp,
  t.tx_id,
  msg_index,
  action_index::NUMERIC AS action_index,
  initial_action,
  current_action,
  sender,
  sender_labels.l1_label AS sender_label_type,
  sender_labels.l2_label AS sender_label_subtype,
  sender_labels.project_name AS sender_address_label,
  sender_labels.address_name AS sender_address_name,
  recipient,
  recipient_labels.l1_label AS recipient_label_type,
  recipient_labels.l2_label AS recipient_label_subtype,
  recipient_labels.project_name AS recipient_label,
  recipient_labels.address_name AS recipient_address_name,
  amount,
  amount * o.price_usd AS amount_usd,
  coalesce(s.symbol, t.denom) AS currency,
  msg_sender,
  contract_address,
  contract_label.address_name AS contract_label
FROM
  {{ ref('silver_terra__transfer_events') }} t
  LEFT OUTER JOIN prices o
  ON DATE_TRUNC('hour', t.block_timestamp) = o.hour
  AND t.denom = o.currency
  
  LEFT OUTER JOIN symbol s
    ON t.denom = s.currency
    
  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS sender_labels
    ON t.sender = sender_labels.address 
    AND sender_labels.blockchain = 'terra' 
    AND sender_labels.creator = 'flipside'

  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS recipient_labels
    ON t.recipient = recipient_labels.address 
    AND recipient_labels.blockchain = 'terra' 
    AND recipient_labels.creator = 'flipside'

  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS contract_label
    ON t.contract_address = contract_label.address 
    AND contract_label.blockchain = 'terra' 
    AND contract_label.creator = 'flipside'