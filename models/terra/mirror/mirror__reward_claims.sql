-- depends_on: {{ ref('silver_terra__msgs') }}
{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'mirror', 'reward_claims']
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
prices_backup AS (
  SELECT
    DATE_TRUNC(
      'day',
      block_timestamp
    ) AS DAY,
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
    blockchain,
    chain_id,
    block_id,
    block_timestamp,
    tx_id,
    msg_value :sender :: STRING AS sender,
    msg_value :contract :: STRING AS contract_address
  FROM
    terra.msgs
  WHERE
    msg_value :execute_msg :withdraw_voting_rewards IS NOT NULL

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
    m.tx_id,
    event_attributes :"1_amount" / pow(
      10,
      6
    ) AS claim_amount,
    event_attributes :"1_contract_address" :: STRING AS claim_currency
  FROM
    {{ ref('silver_terra__msg_events') }}
    e
    JOIN msgs m
    ON m.tx_id = e.tx_id
  WHERE
    event_attributes :"0_action" = 'withdraw'

{% if is_incremental() %}
AND e.block_timestamp :: DATE >= (
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
  m.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  m.tx_id,
  sender,
  claim_amount,
  claim_amount * COALESCE(
    p.price,
    pb.price
  ) AS claim_amount_usd,
  claim_currency,
  contract_address,
  l.address AS contract_label
FROM
  msgs m
  JOIN events e
  ON m.tx_id = e.tx_id
  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
  ON contract_address = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
  LEFT OUTER JOIN prices p
  ON DATE_TRUNC(
    'hour',
    m.block_timestamp
  ) = p.hour
  AND claim_currency = p.currency
  LEFT OUTER JOIN prices_backup pb
  ON DATE_TRUNC(
    'day',
    m.block_timestamp
  ) = pb.day
  AND claim_currency = pb.currency
