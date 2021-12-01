{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'mirror', 'short_farm', 'address_labels']
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
msgs AS(
  SELECT
    m.blockchain,
    chain_id,
    block_id,
    block_timestamp,
    tx_id,
    msg_value :sender :: STRING AS sender,
    msg_value :execute_msg :open_position :collateral_ratio AS collateral_ratio,
    msg_value :contract :: STRING AS contract_address,
    l.address AS contract_label
  FROM
    {{ ref('silver_terra__msgs') }}
    m
    LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
    ON msg_value :contract :: STRING = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
  WHERE
    msg_value :contract :: STRING = 'terra1wfz7h3aqf4cjmjcvc6s8lxdhh7k30nkczyf0mj' --Mirror Mint
    AND msg_value :execute_msg :open_position IS NOT NULL
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
    event_attributes :"0_position_idx" AS collateral_id,
    event_attributes :collateral_amount [0] :amount / pow(
      10,
      6
    ) AS collateral_amount,
    collateral_amount * o.price AS collateral_amount_usd,
    event_attributes :collateral_amount [0] :denom :: STRING AS collateral_currency,
    event_attributes :mint_amount [0] :amount / pow(
      10,
      6
    ) AS mint_amount,
    mint_amount * i.price AS mint_amount_usd,
    event_attributes :mint_amount [0] :denom :: STRING AS mint_currency,
    event_attributes :return_amount / pow(
      10,
      6
    ) AS return_amount,
    return_amount * r.price AS return_amount_usd,
    'uusd' AS return_currency,
    event_attributes :locked_amount [0] :amount / pow(
      10,
      6
    ) AS locked_amount,
    locked_amount * l.price AS locked_amount_usd,
    event_attributes :locked_amount [0] :denom :: STRING AS locked_currency,
    event_attributes :tax_amount / pow(
      10,
      6
    ) AS tax_amount,
    event_attributes :commission_amount / pow(
      10,
      6
    ) AS commission_amount,
    event_attributes :spread_amount / pow(
      10,
      6
    ) AS spread_amount,
    TO_TIMESTAMP(
      event_attributes :unlock_time :: numeric
    ) AS unlock_time
  FROM
    {{ ref('silver_terra__msg_events') }}
    t
    LEFT OUTER JOIN prices o
    ON DATE_TRUNC(
      'hour',
      t.block_timestamp
    ) = o.hour
    AND t.event_attributes :collateral_amount [0] :denom :: STRING = o.currency
    LEFT OUTER JOIN prices i
    ON DATE_TRUNC(
      'hour',
      t.block_timestamp
    ) = i.hour
    AND t.event_attributes :mint_amount [0] :denom :: STRING = i.currency
    LEFT OUTER JOIN prices r
    ON DATE_TRUNC(
      'hour',
      t.block_timestamp
    ) = r.hour
    AND 'uusd' = r.currency
    LEFT OUTER JOIN prices l
    ON DATE_TRUNC(
      'hour',
      t.block_timestamp
    ) = l.hour
    AND t.event_attributes :locked_amount [0] :denom :: STRING = l.currency
  WHERE
    event_type = 'from_contract'
    AND event_attributes :is_short :: STRING = 'true'
    AND event_attributes :from :: STRING = 'terra1wfz7h3aqf4cjmjcvc6s8lxdhh7k30nkczyf0mj' -- Mirror Mint
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
  collateral_id,
  collateral_ratio,
  sender,
  tax_amount,
  commission_amount,
  spread_amount,
  collateral_amount,
  collateral_amount_usd,
  collateral_currency,
  mint_amount,
  mint_amount_usd,
  mint_currency,
  return_amount,
  return_amount_usd,
  return_currency,
  locked_amount,
  locked_amount_usd,
  locked_currency,
  unlock_time,
  contract_address,
  contract_label
FROM
  msgs m
  JOIN events e
  ON m.tx_id = e.tx_id
