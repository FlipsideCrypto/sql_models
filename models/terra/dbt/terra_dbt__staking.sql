{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = "block_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'staking']
) }}

WITH staking_msg_events AS (

  SELECT
    blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp,
    tx_id,
    tx_type,
    msg_module,
    msg_type,
    event_type,
    event_attributes,
    event_attributes :amount AS amount,
    event_attributes :validator :: STRING AS validator
  FROM
    {{ ref('silver_terra__msg_events') }}
  WHERE
    msg_module = 'staking'
    AND event_type = 'delegate'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
),
staking_msg AS (
  SELECT
    blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp,
    tx_id,
    msg_type,
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
    REGEXP_REPLACE(
      msg_value :amount :amount,
      '\"',
      ''
    ) AS event_amount,
    REGEXP_REPLACE(
      msg_value :amount :denom,
      '\"',
      ''
    ) AS event_currency
  FROM
    {{ ref('silver_terra__msgs') }}
  WHERE
    msg_module = 'staking'
    AND msg_type = 'staking/MsgDelegate'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
)
SELECT
  staking_msg.blockchain,
  staking_msg.chain_id,
  staking_msg.tx_status,
  staking_msg.block_id,
  staking_msg.block_timestamp,
  staking_msg.tx_id,
  staking_msg.msg_type,
  delegator_address,
  CASE
    WHEN validator_address IS NULL THEN validator
    ELSE validator_address
  END AS validator_address,
  CASE
    WHEN event_amount IS NULL THEN amount
    ELSE event_amount
  END AS event_amount,
  event_currency
FROM
  staking_msg
  LEFT JOIN staking_msg_events
  ON staking_msg.tx_id = staking_msg_events.tx_id
