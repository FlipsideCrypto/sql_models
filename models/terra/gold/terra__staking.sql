{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = "block_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'staking', 'address_labels']
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
delegate AS (
  SELECT
    blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp,
    tx_id,
    'delegate' AS action,
    REGEXP_REPLACE(
      msg_value :delegator_address,
      '\"',
      ''
    ) AS delegator_address,
    REGEXP_REPLACE(
      msg_value :validator_address,
      '\"',
      ''
    ) AS validator_address,
    REGEXP_REPLACE(msg_value :amount :amount / pow(10, 6), '\"', '') AS amount,
    REGEXP_REPLACE(
      msg_value :amount :denom,
      '\"',
      ''
    ) AS currency
  FROM
    {{ ref('silver_terra__msgs') }}
  WHERE
    msg_module = 'staking'
    AND msg_type = 'staking/MsgDelegate'
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
undelegate AS (
  SELECT
    blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp,
    tx_id,
    'undelegate' AS action,
    REGEXP_REPLACE(
      msg_value :delegator_address,
      '\"',
      ''
    ) AS delegator_address,
    REGEXP_REPLACE(
      msg_value :validator_address,
      '\"',
      ''
    ) AS validator_address,
    REGEXP_REPLACE(msg_value :amount :amount / pow(10, 6), '\"', '') AS amount,
    REGEXP_REPLACE(
      msg_value :amount :denom,
      '\"',
      ''
    ) AS currency
  FROM
    {{ ref('silver_terra__msgs') }}
  WHERE
    msg_module = 'staking'
    AND msg_type = 'staking/MsgUndelegate'
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
redelegate AS (
  SELECT
    blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp,
    tx_id,
    'redelegate' AS action,
    REGEXP_REPLACE(
      msg_value :delegator_address,
      '\"',
      ''
    ) AS delegator_address,
    REGEXP_REPLACE(
      msg_value :validator_dst_address,
      '\"',
      ''
    ) AS validator_address,
    REGEXP_REPLACE(msg_value :amount :amount / pow(10, 6), '\"', '') AS amount,
    REGEXP_REPLACE(
      msg_value :amount :denom,
      '\"',
      ''
    ) AS currency
  FROM
    {{ ref('silver_terra__msgs') }}
  WHERE
    msg_module = 'staking'
    AND msg_type = 'staking/MsgBeginRedelegate'
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
  A.blockchain,
  A.chain_id,
  A.tx_status,
  A.block_id,
  A.block_timestamp,
  A.tx_id,
  A.action,
  A.delegator_address,
  delegator_labels.l1_label AS delegator_label_type,
  delegator_labels.l2_label AS delegator_label_subtype,
  delegator_labels.project_name AS delegator_address_label,
  delegator_labels.address AS delegator_address_name,
  A.validator_address,
  validator_labels.l1_label AS validator_label_type,
  validator_labels.l2_label AS validator_label_subtype,
  validator_labels.project_name AS validator_address_label,
  validator_labels.address AS validator_address_name,
  A.amount :: FLOAT AS event_amount,
  price_usd,
  A.amount * price_usd AS event_amount_usd,
  p.symbol AS currency
FROM
  (
    SELECT
      *
    FROM
      delegate
    UNION ALL
    SELECT
      *
    FROM
      undelegate
    UNION ALL
    SELECT
      *
    FROM
      redelegate
  ) A
  LEFT OUTER JOIN prices p
  ON p.currency = A.currency
  AND p.hour = DATE_TRUNC(
    'hour',
    A.block_timestamp
  )
  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }}
  delegator_labels
  ON A.delegator_address = delegator_labels.address AND delegator_labels.blockchain = 'terra' AND delegator_labels.creator = 'flipside'
  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }}
  validator_labels
  ON A.validator_address = validator_labels.address AND validator_labels.blockchain = 'terra' AND validator_labels.creator = 'flipside'
