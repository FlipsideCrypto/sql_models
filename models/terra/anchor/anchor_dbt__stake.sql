{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = "block_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'anchor_dbt', 'stake']
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
    'unstake' AS action,
    msg_value :sender :: STRING AS sender,
    msg_value :execute_msg :withdraw_voting_tokens :amount / pow(
      10,
      6
    ) AS amount,
    msg_value :contract :: STRING AS contract_address,
    l.address AS contract_label
  FROM
    {{ ref('silver_terra__msgs') }}
    m
    LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
    ON msg_value :contract :: STRING = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
  WHERE
    msg_value :execute_msg :withdraw_voting_tokens IS NOT NULL
    AND msg_value :contract :: STRING = 'terra1f32xyep306hhcxxxf7mlyh0ucggc00rm2s9da5'
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
    price,
    event_attributes :"0_contract_address" :: STRING AS currency
  FROM
    {{ ref('silver_terra__msg_events') }}
    LEFT OUTER JOIN prices r
    ON DATE_TRUNC(
      'hour',
      block_timestamp
    ) = HOUR
    AND event_attributes :"0_contract_address" :: STRING = r.currency
  WHERE
    event_type = 'execute_contract'
    AND tx_id IN(
      SELECT
        tx_id
      FROM
        msgs
    )
    AND msg_index = 0
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
  action,
  sender,
  amount,
  amount * price AS amount_usd,
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
  'stake' AS action,
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
  LEFT OUTER JOIN prices r
  ON DATE_TRUNC(
    'hour',
    block_timestamp
  ) = HOUR
  AND msg_value :contract :: STRING = r.currency
WHERE
  msg_value :execute_msg :send :msg :stake_voting_tokens IS NOT NULL
  AND msg_value :execute_msg :send :contract :: STRING = 'terra1f32xyep306hhcxxxf7mlyh0ucggc00rm2s9da5'
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
