{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = "block_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'transfers']
) }}

WITH prices AS (

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
),
symbol AS (
  SELECT
    currency,
    symbol
  FROM
    {{ ref('terra__oracle_prices') }}
  WHERE
    block_timestamp >= CURRENT_DATE - 2
  GROUP BY
    1,
    2
),
inputs AS(
  SELECT
    blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp,
    tx_id,
    msg_type,
    A.value :address :: STRING AS event_from,
    A.value :coins [0] :amount / pow(
      10,
      6
    ) AS event_amount,
    A.value :coins [0] :denom :: STRING AS event_currency,
    A.index AS input_index
  FROM
    {{ ref('silver_terra__msgs') }},
    LATERAL FLATTEN(
      input => msg_value :inputs
    ) A
  WHERE
    msg_module = 'bank'
    AND msg_type = 'bank/MsgMultiSend'
    AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
),
outputs AS(
  SELECT
    tx_id,
    A.value :address :: STRING AS event_to,
    A.index AS output_index
  FROM
    {{ ref('silver_terra__msgs') }},
    LATERAL FLATTEN(
      input => msg_value :outputs
    ) A
  WHERE
    msg_module = 'bank'
    AND msg_type = 'bank/MsgMultiSend'
    AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
),
transfers AS(
  SELECT
    blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp,
    i.tx_id,
    msg_type,
    event_from,
    event_to,
    event_amount,
    event_currency
  FROM
    inputs i
    JOIN outputs o
    ON i.tx_id = o.tx_id
    AND i.input_index = o.output_index
  UNION
  SELECT
    blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp,
    tx_id,
    msg_type,
    msg_value :from_address :: STRING AS event_from,
    msg_value :to_address :: STRING AS event_to,
    msg_value :amount [0] :amount / pow(
      10,
      6
    ) AS event_amount,
    msg_value :amount [0] :denom :: STRING AS event_currency
  FROM
    {{ ref('silver_terra__msgs') }}
  WHERE
    msg_module = 'bank'
    AND msg_type = 'bank/MsgSend'
    AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
UNION
SELECT
  blockchain,
  chain_id,
  tx_status,
  block_id,
  block_timestamp,
  tx_id,
  msg_type,
  msg_value :sender :: STRING AS event_from,
  msg_value :execute_msg :transfer :recipient :: STRING AS event_to,
  msg_value :execute_msg :transfer :amount / pow(
    10,
    6
  ) AS event_amount,
  msg_value :contract :: STRING AS event_currency
FROM
  {{ ref('silver_terra__msgs') }}
WHERE
  msg_value :execute_msg :transfer IS NOT NULL
  AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
)
SELECT
  t.blockchain,
  t.chain_id,
  t.tx_status,
  t.block_id,
  t.block_timestamp,
  t.tx_id,
  t.msg_type,
  t.event_from,
  from_labels.l1_label AS event_from_label_type,
  from_labels.l2_label AS event_from_label_subtype,
  from_labels.project_name AS event_from_address_label,
  from_labels.address_name AS event_from_address_name,
  t.event_to,
  to_labels.l1_label AS event_to_label_type,
  to_labels.l2_label AS event_to_label_subtype,
  to_labels.project_name AS event_to_address_label,
  to_labels.address_name AS event_to_address_name,
  t.event_amount,
  t.event_amount * price_usd AS event_amount_usd,
  s.symbol AS event_currency
FROM
  transfers t
  LEFT OUTER JOIN prices o
  ON DATE_TRUNC(
    'hour',
    t.block_timestamp
  ) = o.hour
  AND t.event_currency = o.currency
  LEFT OUTER JOIN symbol s
  ON t.event_currency = s.currency
  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS from_labels
  ON event_from = from_labels.address
  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS to_labels
  ON event_to = to_labels.address
