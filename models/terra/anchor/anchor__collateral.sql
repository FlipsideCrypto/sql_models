{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'anchor', 'collateral']
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
    msg_value :execute_msg :send :contract :: STRING AS contract_address,
    l.address AS contract_label
  FROM
    {{ ref('silver_terra__msgs') }}
    m
    LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
    ON msg_value :execute_msg :send :contract :: STRING = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
  WHERE
    msg_value :execute_msg :withdraw_collateral IS NOT NULL
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
events AS (
  SELECT
    tx_id,
    event_attributes :collaterals [0] :amount / pow(
      10,
      6
    ) AS amount,
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
  contract_address,
  contract_label
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
  msg_value :execute_msg :send :amount / pow(
    10,
    6
  ) AS amount,
  amount * price AS amount_usd,
  msg_value :contract :: STRING AS currency,
  msg_value :execute_msg :send :contract :: STRING AS contract_address,
  l.address AS contract_label
FROM
  {{ ref('silver_terra__msgs') }}
  m
  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
  ON msg_value :execute_msg :send :contract :: STRING = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
  LEFT OUTER JOIN prices o
  ON DATE_TRUNC(
    'hour',
    block_timestamp
  ) = o.hour
  AND msg_value :contract :: STRING = o.currency
WHERE
  msg_value :execute_msg :send :msg :deposit_collateral IS NOT NULL
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
