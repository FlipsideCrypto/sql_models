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
)

SELECT 
  DISTINCT * 
FROM swaps