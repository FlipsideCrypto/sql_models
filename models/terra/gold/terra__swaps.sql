{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = "block_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'swap']
) }}

WITH msgs AS(

  SELECT
    blockchain,
    chain_id,
    block_id,
    block_timestamp,
    tx_status,
    tx_id,
    msg_index,
    msg_type,
    msg_value :trader :: STRING AS trader,
    msg_value :ask_denom :: STRING AS ask_currency,
    msg_value :offer_coin :amount :: NUMBER AS offer_amount,
    msg_value :offer_coin :denom :: STRING AS offer_currency,
    msg_value
  FROM
    {{ ref('silver_terra__msgs') }}
  WHERE
    msg_module = 'market'
    AND msg_type = 'market/MsgSwap'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
),
events_transfer AS(
  SELECT
    tx_id,
    event_type,
    event_attributes,
    msg_index,
    -- event_index,
    event_attributes :"0_sender" :: STRING AS "0_sender",
    event_attributes :"0_recipient" :: STRING AS "0_recipient",
    event_attributes :"0_amount" [0] :amount :: NUMBER AS "0_amount",
    event_attributes :"0_amount" [0] :denom :: STRING AS "0_amount_currency",
    event_attributes :"1_sender" :: STRING AS "1_sender",
    event_attributes :"1_recipient" :: STRING AS "1_recipient",
    event_attributes :"1_amount" [0] :amount :: NUMBER AS "1_amount",
    event_attributes :"1_amount" [0] :denom :: STRING AS "1_amount_currency"
  FROM
    {{ ref('silver_terra__msg_events') }}
  WHERE
    event_type = 'transfer'
    AND msg_type = 'market/MsgSwap'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
),
fees AS(
  SELECT
    tx_id,
    event_type,
    msg_index,
    -- event_index,
    event_attributes :swap_fee [0] :amount AS swap_fee_amount,
    event_attributes :swap_fee [0] :denom :: STRING AS swap_fee_currency
  FROM
    {{ ref('silver_terra__msg_events') }}
  WHERE
    event_type = 'swap'
    AND msg_type = 'market/MsgSwap'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
),
prices AS (
  SELECT
    DATE_TRUNC(
      'hour',
      block_timestamp
    ) AS HOUR,
    currency,
    symbol,
    AVG(price_usd) AS price_usd
  FROM
    {{ ref('terra__oracle_prices') }}
  GROUP BY
    1,
    2,
    3
)
SELECT
  m.blockchain,
  m.chain_id,
  m.block_id,
  m.block_timestamp,
  m.tx_status,
  m.tx_id,
  m.msg_index,
  f.swap_fee_amount / pow(
    10,
    6
  ) AS swap_fee_amount,
  f.swap_fee_amount / pow(
    10,
    6
  ) * fe.price_usd AS swap_fee_amount_usd,
  fe.symbol AS swap_fee_currency,
  m.trader,
  aa.symbol AS ask_currency,
  m.offer_amount / pow(
    10,
    6
  ) AS offer_amount,
  m.offer_amount / pow(
    10,
    6
  ) * oo.price_usd AS offer_amount_usd,
  oo.symbol AS offer_currency,
  et."0_amount" / pow(
    10,
    6
  ) AS token_0_amount,
  token_0_amount * z.price_usd AS token_0_amount_usd,
  z.symbol AS token_0_currency,
  et."1_amount" / pow(
    10,
    6
  ) AS token_1_amount,
  token_1_amount * o.price_usd AS token_1_amount_usd,
  o.symbol AS token_1_currency,
  z.price_usd AS price0_usd,
  o.price_usd AS price1_usd,
  token_0_currency || ' to ' || token_1_currency AS swap_pair
FROM
  msgs m
  LEFT OUTER JOIN events_transfer et
  ON m.tx_id = et.tx_id
  AND m.msg_index = et.msg_index
  LEFT OUTER JOIN fees f
  ON m.tx_id = f.tx_id
  AND m.msg_index = f.msg_index
  LEFT OUTER JOIN prices z
  ON DATE_TRUNC(
    'hour',
    m.block_timestamp
  ) = z.hour
  AND et."0_amount_currency" = z.currency
  LEFT OUTER JOIN prices o
  ON DATE_TRUNC(
    'hour',
    m.block_timestamp
  ) = o.hour
  AND et."1_amount_currency" = o.currency
  LEFT OUTER JOIN prices fe
  ON DATE_TRUNC(
    'hour',
    m.block_timestamp
  ) = fe.hour
  AND f.swap_fee_currency = fe.currency
  LEFT OUTER JOIN prices oo
  ON DATE_TRUNC(
    'hour',
    m.block_timestamp
  ) = oo.hour
  AND m.offer_currency = oo.currency
  LEFT OUTER JOIN prices aa
  ON DATE_TRUNC(
    'hour',
    m.block_timestamp
  ) = aa.hour
  AND m.ask_currency = aa.currency
WHERE
  tx_status = 'SUCCEEDED'
