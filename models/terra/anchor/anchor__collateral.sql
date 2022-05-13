{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'anchor', 'collateral', 'address_labels']
) }}

WITH prices AS (

  SELECT
    DATE_TRUNC(
      'hour',
      block_timestamp
    ) AS HOUR,
    currency,
    symbol,
    AVG(price_usd) AS price
  FROM
    {{ ref('terra__oracle_prices') }}
  WHERE
    1 = 1

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
  SELECT
    MAX(
      block_timestamp :: DATE
    )
  FROM
    {{ ref('silver_terra__msgs') }}
)
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
    'withdraw' AS action,
    msg_value :sender :: STRING AS sender,
    COALESCE(msg_value :execute_msg :send :contract :: STRING, msg_value :contract :: STRING) AS contract_address,
    l.address_name AS contract_label
  FROM
    {{ ref('silver_terra__msgs') }}
    m
    LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
    ON COALESCE(msg_value :execute_msg :send :contract :: STRING, msg_value :contract :: STRING) = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
  WHERE
    msg_value :execute_msg :withdraw_collateral IS NOT NULL
    AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
AND
  block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
),
events AS (
  SELECT
    tx_id,
    CASE WHEN event_attributes :collaterals [0] :denom :: STRING = 'terra1z3e2e4jpk4n0xzzwlkgcfvc95pc5ldq0xcny58' -- whsAVAX
    THEN event_attributes :collaterals [0] :amount / pow(
      10,
      8
    ) ELSE event_attributes :collaterals [0] :amount / pow(
      10,
      6
    ) END AS amount,
    amount * price AS amount_usd,
    event_attributes :collaterals [0] :denom :: STRING AS currency
  FROM
    {{ ref('silver_terra__msg_events') }}
    m
    LEFT OUTER JOIN prices o
    ON DATE_TRUNC(
      'hour',
      block_timestamp
    ) = o.hour
    AND event_attributes :collaterals [0] :denom :: STRING = o.currency
  WHERE
    tx_id IN(
      SELECT
        tx_id
      FROM
        msgs
    )
    AND event_type = 'from_contract'
    AND event_attributes :collaterals IS NOT NULL
    AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
  SELECT
    MAX(
      block_timestamp :: DATE
    )
  FROM
    {{ ref('silver_terra__msgs') }}
)
{% endif %}
),

wormhole_msgs AS (
SELECT DISTINCT
tx_id
FROM  {{ ref('silver_terra__msgs') }}
WHERE msg_value :execute_msg :process_anchor_message IS NOT NULL
AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
AND
  block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
),

wormhole_deposits AS (
SELECT 
a.blockchain,
a.block_id,
a.block_timestamp,
a.chain_id,
a.tx_id,
a.msg_index,
'provide' AS event_type,
action_log :borrower::STRING AS sender,
CASE WHEN event_attributes :collaterals [0] :denom::STRING = 'terra1z3e2e4jpk4n0xzzwlkgcfvc95pc5ldq0xcny58' --whsAVAX
THEN action_log :amount / POW(
  10,
  8
  ) ELSE action_log :amount / POW(
  10,
  6
  ) END AS amount,
action_contract_address AS contract_address,
event_attributes :collaterals [0] :denom::STRING as currency
FROM {{ ref('silver_terra__event_actions') }} a
LEFT JOIN {{ ref('silver_terra__msg_events') }} e
ON a.tx_id = e.tx_id AND a.msg_index = e.msg_index
WHERE a.tx_id IN (SELECT tx_id FROM wormhole_msgs)
AND action_method = 'lock_collateral'
AND e.event_type = 'from_contract'
AND action_log :amount IS NOT NULL 

{% if is_incremental() %}
AND a.block_timestamp :: DATE >= (
  SELECT
    MAX(
      block_timestamp :: DATE
    )
  FROM
    {{ ref('silver_terra__msgs') }}
)
{% endif %}
),

wormhole_withdraws AS (
SELECT 
a.blockchain,
a.block_id,
a.block_timestamp,
a.chain_id,
a.tx_id,
a.msg_index,
'withdraw' AS event_type,
action_log :borrower::STRING AS sender,
CASE WHEN event_attributes :collaterals [0] :denom::STRING = 'terra1z3e2e4jpk4n0xzzwlkgcfvc95pc5ldq0xcny58' --whsAVAX
THEN action_log :amount / POW(
  10,
  8
  ) ELSE action_log :amount / POW(
  10,
  6
  ) END AS amount,
action_contract_address AS contract_address,
event_attributes :collaterals [0] :denom::STRING AS currency
FROM {{ ref('silver_terra__event_actions') }} a
LEFT JOIN {{ ref('silver_terra__msg_events') }} e
ON a.tx_id = e.tx_id AND a.msg_index = e.msg_index
WHERE a.tx_id IN (SELECT tx_id FROM wormhole_msgs)
AND action_method = 'withdraw_collateral'
AND e.event_type = 'from_contract'

{% if is_incremental() %}
AND a.block_timestamp :: DATE >= (
  SELECT
    MAX(
      block_timestamp :: DATE
    )
  FROM
    {{ ref('silver_terra__msgs') }}
)
{% endif %}

)

SELECT
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  m.tx_id,
  action AS event_type,
  sender,
  amount,
  amount_usd,
  currency,
  contract_address AS contract_address,
  COALESCE(contract_label, '') AS contract_label,
  'Terra' AS source
FROM
  msgs m
  JOIN events e
  ON m.tx_id = e.tx_id

UNION

SELECT
  m.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  'provide' AS event_type,
  msg_value :sender :: STRING AS sender,
  CASE WHEN msg_value :contract :: STRING = 'terra1z3e2e4jpk4n0xzzwlkgcfvc95pc5ldq0xcny58' --whsAVAX
  THEN msg_value :execute_msg :send :amount / pow(
    10,
    8
  ) 
  ELSE msg_value :execute_msg :send :amount / pow(
    10,
    6
  ) END AS amount,
  amount * price AS amount_usd,
  msg_value :contract :: STRING AS currency,
  COALESCE(msg_value :execute_msg :send :contract :: STRING, msg_value :contract :: STRING) AS contract_address,
  COALESCE(l.address_name, '') AS contract_label,
  'Terra' AS source
FROM
  {{ ref('silver_terra__msgs') }}
  m
  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
  ON COALESCE(msg_value :execute_msg :send :contract :: STRING, msg_value :contract :: STRING) = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
  LEFT OUTER JOIN prices o
  ON DATE_TRUNC(
    'hour',
    block_timestamp
  ) = o.hour
  AND msg_value :contract :: STRING = o.currency
WHERE
  tx_id in (select tx_id from {{ ref('silver_terra__msgs') }} where msg_value:execute_msg:lock_collateral is not null)
  and tx_status = 'SUCCEEDED'
  and msg_value :execute_msg :send :contract::STRING IN ('terra1ptjp2vfjrwh0j0faj9r6katm640kgjxnwwq9kn', 'terra10cxuzggyvvv44magvrh3thpdnk9cmlgk93gmx2',
                                                          'terra1zdxlrtyu74gf6pvjkg9t22hentflmfcs86llva') --Anchor Custody and bATOM Contracts

{% if is_incremental() %}
AND
  block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}

UNION

SELECT
  d.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  event_type,
  sender,
  amount,
  amount * price AS amount_usd,
  d.currency,
  contract_address,
  COALESCE(l.address_name, '') AS contract_label,
  'Wormhole' AS source
FROM
  wormhole_deposits d
LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
  ON d.contract_address = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
  LEFT OUTER JOIN prices o
  ON DATE_TRUNC(
    'hour',
    block_timestamp
  ) = o.hour
  AND d.currency = o.currency

  UNION

  SELECT
  w.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  event_type,
  sender,
  amount,
  amount * price AS amount_usd,
  w.currency,
  contract_address,
  COALESCE(l.address_name, '') AS contract_label,
  'Wormhole' AS source
FROM
  wormhole_withdraws w
LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
  ON w.contract_address = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
  LEFT OUTER JOIN prices o
  ON DATE_TRUNC(
    'hour',
    block_timestamp
  ) = o.hour
  AND w.currency = o.currency