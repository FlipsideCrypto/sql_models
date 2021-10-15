{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = 'block_id',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp'],
  tags = ['snowflake', 'terra', 'staking']
) }}

WITH delegate AS (

  SELECT
    blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp,
    tx_id,
    'delegate' AS action,
    delegator_address,
    validator_address,
    event_amount AS amount,
    event_currency AS currency
  FROM
    {{ ref('terra_dbt__delegate') }}
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
    delegator_address,
    validator_address,
    event_amount AS amount,
    event_currency AS currency
  FROM
    {{ ref('terra_dbt__undelegate') }}
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
    delegator_address,
    validator_dst_address AS validator_address,
    event_amount AS amount,
    event_currency AS currency
  FROM
    {{ ref('terra_dbt__redelegate') }}
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
  delegator_labels.address_name AS delegator_address_name,
  A.validator_address,
  validator_labels.l1_label AS validator_label_type,
  validator_labels.l2_label AS validator_label_subtype,
  validator_labels.project_name AS validator_address_label,
  validator_labels.address_name AS validator_address_name,
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
  LEFT OUTER JOIN {{ source(
    'shared',
    'udm_address_labels_new'
  ) }}
  delegator_labels
  ON A.delegator_address = delegator_labels.address
  LEFT OUTER JOIN {{ source(
    'shared',
    'udm_address_labels_new'
  ) }}
  validator_labels
  ON A.validator_address = validator_labels.address
