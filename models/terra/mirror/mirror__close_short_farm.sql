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
tx AS (
  SELECT
    blockchain,
    chain_id,
    block_id,
    block_timestamp,
    tx_id,
    msg_value
  FROM
    {{ ref('silver_terra__msgs') }}
  WHERE
    msg_value :execute_msg :send :msg IS NOT NULL
    AND msg_value :execute_msg :send :contract :: STRING = 'terra1wfz7h3aqf4cjmjcvc6s8lxdhh7k30nkczyf0mj'
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
event_tx AS (
  SELECT
    block_timestamp,
    tx_id,
    event_attributes
  FROM
    {{ ref('silver_terra__msg_events') }}
  WHERE
    tx_id IN(
      SELECT
        tx_id
      FROM
        tx
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
),
msgs AS (
  SELECT
    t.blockchain,
    chain_id,
    block_id,
    block_timestamp,
    tx_id,
    msg_value :sender :: STRING AS sender,
    msg_value :execute_msg :send :contract :: STRING AS contract_address,
    l.address_name AS contract_label
  FROM
    tx t
    LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
    ON msg_value :execute_msg :send :contract :: STRING = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
),
withdraw_events AS (
  SELECT
    tx_id,
    event_attributes :"0_position_idx" :: INTEGER AS collateral_id,
    event_attributes :withdraw_amount [0] :amount / pow(
      10,
      6
    ) AS withdraw_amount,
    withdraw_amount * COALESCE(
      o.price,
      o_b.price
    ) AS withdraw_amount_usd,
    event_attributes :withdraw_amount [0] :denom :: STRING AS withdraw_currency,
    event_attributes :unlocked_amount [0] :amount / pow(
      10,
      6
    ) AS unlocked_amount,
    unlocked_amount * COALESCE(
      i.price,
      i_b.price
    ) AS unlocked_amount_usd,
    event_attributes :unlocked_amount [0] :denom :: STRING AS unlocked_currency,
    (
      event_attributes :"0_tax_amount" [0] :amount + event_attributes :"1_tax_amount" [0] :amount
    ) / pow(
      10,
      6
    ) AS tax_amount,
    tax_amount * COALESCE(
      A.price,
      a_b.price
    ) AS tax_amount_usd,
    event_attributes :"0_tax_amount" [0] :denom :: STRING AS tax_currency
  FROM
    event_tx t
    LEFT OUTER JOIN prices o
    ON DATE_TRUNC(
      'hour',
      t.block_timestamp
    ) = o.hour
    AND t.event_attributes :withdraw_amount [0] :denom :: STRING = o.currency
    LEFT OUTER JOIN prices i
    ON DATE_TRUNC(
      'hour',
      t.block_timestamp
    ) = i.hour
    AND t.event_attributes :unlocked_amount [0] :denom :: STRING = i.currency
    LEFT OUTER JOIN prices A
    ON DATE_TRUNC(
      'hour',
      t.block_timestamp
    ) = A.hour
    AND t.event_attributes :"0_tax_amount" [0] :denom :: STRING = A.currency
    LEFT OUTER JOIN prices_backup o_b
    ON DATE_TRUNC(
      'day',
      t.block_timestamp
    ) = o_b.day
    AND t.event_attributes :withdraw_amount [0] :denom :: STRING = o_b.currency
    LEFT OUTER JOIN prices_backup i_b
    ON DATE_TRUNC(
      'day',
      t.block_timestamp
    ) = i_b.day
    AND t.event_attributes :unlocked_amount [0] :denom :: STRING = i_b.currency
    LEFT OUTER JOIN prices_backup a_b
    ON DATE_TRUNC(
      'day',
      t.block_timestamp
    ) = a_b.day
    AND t.event_attributes :"0_tax_amount" [0] :denom :: STRING = a_b.currency
  WHERE
    event_attributes :withdraw_amount[0] IS NOT NULL
),
burn_events AS (
  SELECT
    tx_id,
    event_attributes :position_idx :: INTEGER as collateral_id,
    event_attributes :burn_amount [0] :amount / pow(
      10,
      6
    ) AS burn_amount,
    burn_amount * o.price AS burn_amount_usd,
    event_attributes :burn_amount [0] :denom :: STRING AS burn_currency,
    event_attributes :protocol_fee [0] :amount / pow(
      10,
      6
    ) AS protocol_fee_amount,
    protocol_fee_amount * i.price AS protocol_fee_amount_usd,
    event_attributes :protocol_fee [0] :denom :: STRING AS protocol_fee_currency
  FROM
    event_tx t
    LEFT OUTER JOIN prices o
    ON DATE_TRUNC(
      'hour',
      t.block_timestamp
    ) = o.hour
    AND t.event_attributes :burn_amount [0] :denom :: STRING = o.currency
    LEFT OUTER JOIN prices i
    ON DATE_TRUNC(
      'hour',
      t.block_timestamp
    ) = i.hour
    AND t.event_attributes :protocol_fee [0] :denom :: STRING = i.currency
  WHERE
    event_attributes :burn_amount IS NOT NULL
)
SELECT
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  m.tx_id,
  COALESCE(
    w.collateral_id,
    b.collateral_id
  ) AS collateral_id,
  sender,
  tax_amount,
  tax_amount_usd,
  tax_currency,
  protocol_fee_amount,
  protocol_fee_amount_usd,
  protocol_fee_currency,
  burn_amount,
  burn_amount_usd,
  burn_currency,
  withdraw_amount,
  withdraw_amount_usd,
  withdraw_currency,
  unlocked_amount,
  unlocked_amount_usd,
  unlocked_currency,
  contract_address,
  contract_label
FROM
  msgs m
  JOIN withdraw_events w
  ON m.tx_id = w.tx_id
  JOIN burn_events b
  ON m.tx_id = b.tx_id
WHERE
  unlocked_amount IS NOT NULL
