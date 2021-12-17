{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'mirror', 'liquidations', 'address_labels']
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
    blockchain,
    chain_id,
    block_id,
    block_timestamp,
    tx_id,
    msg_value :sender :: STRING AS sender,
    msg_value :execute_msg :send :contract :: STRING AS contract_address
  FROM
    {{ ref('silver_terra__msgs') }}
  WHERE
    msg_value :execute_msg :send :contract :: STRING = 'terra1wfz7h3aqf4cjmjcvc6s8lxdhh7k30nkczyf0mj' -- Mirror Mint Contract
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
    COALESCE(
      (
        event_attributes :"0_tax_amount" [0] :amount + event_attributes :"1_tax_amount" [0] :amount
      ),
      event_attributes :tax_amount [0] :amount
    ) / pow(
      10,
      6
    ) AS tax_amount,
    COALESCE(
      event_attributes :"1_tax_amount" [0] :denom :: STRING,
      event_attributes :tax_amount [0] :denom :: STRING
    ) AS tax_currency,
    event_attributes :protocol_fee [0] :amount / pow(
      10,
      6
    ) AS protocol_fee_amount,
    event_attributes :protocol_fee [0] :denom :: STRING AS protocol_fee_currency,
    event_attributes :liquidated_amount [0] :amount / pow(
      10,
      6
    ) AS liquidated_amount,
    event_attributes :liquidated_amount [0] :denom :: STRING AS liquidated_currency,
    event_attributes :return_collateral_amount [0] :amount / pow(
      10,
      6
    ) AS return_collateral_amount,
    event_attributes :return_collateral_amount [0] :denom :: STRING AS return_collateral_currency,
    event_attributes :unlocked_amount [0] :amount / pow(
      10,
      6
    ) AS unlocked_amount,
    event_attributes :unlocked_amount [0] :denom :: STRING AS unlocked_currency,
    COALESCE(
    event_attributes :position_idx :: INTEGER,
    event_attributes :"0_position_idx" :: INTEGER
    ) AS collateral_id,
    event_attributes :owner :: STRING AS owner
  FROM
    {{ ref('silver_terra__msg_events') }}
  WHERE
    event_type = 'from_contract'
    AND tx_id IN(
      SELECT
        tx_id
      FROM
        msgs
    )
    AND event_attributes :return_collateral_amount IS NOT NULL
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
  m.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  e.tx_id,
  collateral_id,
  sender AS buyer,
  owner,
  tax_amount,
  tax_amount * t.price AS tax_amount_usd,
  tax_currency,
  protocol_fee_amount,
  protocol_fee_amount * p.price AS protocol_fee_amount_usd,
  protocol_fee_currency,
  liquidated_amount,
  liquidated_amount * l.price AS liquidated_amount_usd,
  liquidated_currency,
  return_collateral_amount,
  return_collateral_amount * r.price AS return_collateral_amount_usd,
  return_collateral_currency,
  unlocked_amount,
  unlocked_amount * u.price AS unlocked_amount_usd,
  unlocked_currency,
  contract_address,
  g.address_name AS contract_label
FROM
  events e
  JOIN msgs m
  ON e.tx_id = m.tx_id
  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS g
  ON m.contract_address = g.address AND g.blockchain = 'terra' AND g.creator = 'flipside'
  LEFT OUTER JOIN prices t
  ON DATE_TRUNC(
    'hour',
    m.block_timestamp
  ) = t.hour
  AND tax_currency = t.currency
  LEFT OUTER JOIN prices p
  ON DATE_TRUNC(
    'hour',
    m.block_timestamp
  ) = p.hour
  AND protocol_fee_currency = p.currency
  LEFT OUTER JOIN prices l
  ON DATE_TRUNC(
    'hour',
    m.block_timestamp
  ) = l.hour
  AND liquidated_currency = l.currency
  LEFT OUTER JOIN prices r
  ON DATE_TRUNC(
    'hour',
    m.block_timestamp
  ) = r.hour
  AND return_collateral_currency = r.currency
  LEFT OUTER JOIN prices u
  ON DATE_TRUNC(
    'hour',
    m.block_timestamp
  ) = u.hour
  AND unlocked_currency = u.currency
