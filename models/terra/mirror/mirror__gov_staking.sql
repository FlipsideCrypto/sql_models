{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-',block_id, tx_id, msg_index)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'mirror', 'mirror_gov', 'address_labels']
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
stake_msgs AS (
  SELECT
    t.blockchain,
    chain_id,
    block_id,
    t.block_timestamp,
    t.tx_id,
    t.msg_index,
    msg_value :sender :: STRING AS sender,
    msg_value :execute_msg :send :amount / pow(
      10,
      6
    ) AS event_amount,
    event_amount * o.price AS event_amount_usd,
    msg_value :contract :: STRING AS event_currency,
    msg_value :execute_msg :send :contract :: STRING AS contract_address,
    l.address AS contract_label
  FROM
    {{ ref('silver_terra__msgs') }}
    t
    LEFT OUTER JOIN prices o
    ON DATE_TRUNC(
      'hour',
      t.block_timestamp
    ) = o.hour
    AND msg_value :contract :: STRING = o.currency
    LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
    ON msg_value :execute_msg :send :contract :: STRING = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
  WHERE
    msg_value :execute_msg :send :msg :stake_voting_tokens IS NOT NULL
    AND msg_value :execute_msg :send :contract :: STRING = 'terra1wh39swv7nq36pnefnupttm2nr96kz7jjddyt2x'
    AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
AND t.block_timestamp :: DATE >= (
  SELECT
    MAX(
      block_timestamp :: DATE
    )
  FROM
    {{ ref('silver_terra__msgs') }}
)
{% endif %}
),
stake_events AS (
  SELECT
    tx_id,
    msg_index,
    event_attributes :share :: FLOAT AS shares
  FROM
    {{ ref('silver_terra__msg_events') }}
  WHERE
    tx_id IN(
      SELECT
        DISTINCT tx_id
      FROM
        stake_msgs
    )
    AND event_type = 'from_contract'

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
) -- Staking
SELECT
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  m.tx_id,
  e.msg_index,
  'stake' AS event_type,
  sender,
  event_amount,
  event_amount_usd,
  event_currency,
  shares,
  contract_address,
  contract_label
FROM
  stake_msgs m
  JOIN stake_events e
  ON m.tx_id = e.tx_id
  AND m.msg_index = e.msg_index
UNION
  -- Unstaking
SELECT
  t.blockchain,
  t.chain_id,
  t.block_id,
  t.block_timestamp,
  t.tx_id,
  t.msg_index,
  'unstake' AS event_type,
  msg_value :sender :: STRING AS sender,
  CASE
    WHEN msg_value :execute_msg :withdraw_voting_tokens :amount IS NULL THEN q.event_attributes :"0_amount"
    ELSE msg_value :execute_msg :withdraw_voting_tokens :amount
  END / pow(
    10,
    6
  ) AS event_amount,
  event_amount * o.price AS event_amount_usd,
  'terra15gwkyepfc6xgca5t5zefzwy42uts8l2m4g40k6' AS event_currency,
  NULL AS shares,
  msg_value :contract :: STRING AS contract_address,
  l.address AS contract_label
FROM
  {{ ref('silver_terra__msgs') }}
  t
  LEFT JOIN {{ ref('silver_terra__msg_events') }}
  q
  ON t.tx_id = q.tx_id
  AND q.event_type = 'from_contract'
  LEFT OUTER JOIN prices o
  ON DATE_TRUNC(
    'hour',
    t.block_timestamp
  ) = o.hour
  AND 'terra15gwkyepfc6xgca5t5zefzwy42uts8l2m4g40k6' = o.currency
  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
  ON msg_value :contract :: STRING = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
WHERE
  msg_value :execute_msg :withdraw_voting_tokens IS NOT NULL
  AND msg_value :contract :: STRING = 'terra1wh39swv7nq36pnefnupttm2nr96kz7jjddyt2x'
  AND t.tx_status = 'SUCCEEDED'

{% if is_incremental() %}
AND t.block_timestamp :: DATE >= (
  SELECT
    MAX(
      block_timestamp :: DATE
    )
  FROM
    {{ ref('silver_terra__msgs') }}
)
{% endif %}
