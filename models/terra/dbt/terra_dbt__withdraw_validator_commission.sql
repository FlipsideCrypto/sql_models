{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = "block_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'reward']
) }}

WITH rewards_event AS (

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
    msg_index,
    event_type,
    event_attributes,
    VALUE :amount / pow(
      10,
      6
    ) AS event_rewards_amount,
    VALUE :denom :: STRING AS event_rewards_currency,
     event_attributes :receiver :: STRING AS delegator_address
  FROM
    {{ ref('silver_terra__msg_events') }},
    LATERAL FLATTEN(
      input => event_attributes :amount
    )
  WHERE
    msg_module = 'distribution'
    AND msg_type = 'distribution/MsgWithdrawValidatorCommission'
    AND event_type = 'coin_received'
    AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
),
rewards AS (
  SELECT
    blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp,
    tx_id,
    msg_type,
    msg_index,
    REGEXP_REPLACE(
      msg_value :validator_address,
      '\"',
      ''
    ) AS validator_address,
    REGEXP_REPLACE(msg_value :amount :amount / pow(10, 6), '\"', '') AS event_amount,
    REGEXP_REPLACE(
      msg_value :amount :denom,
      '\"',
      ''
    ) AS event_currency
  FROM
    {{ ref('silver_terra__msgs') }}
  WHERE
    msg_module = 'distribution'
    AND msg_type = 'distribution/MsgWithdrawValidatorCommission'
    AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
),
rewards_event_base AS (
  SELECT
    DISTINCT blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp,
    tx_id,
    msg_type,
    msg_index,
    event_rewards_amount,
    event_rewards_currency,
    delegator_address
  FROM
    rewards_event
)
SELECT
  DISTINCT rewards_event_base.blockchain,
  rewards_event_base.chain_id,
  rewards_event_base.tx_status,
  rewards_event_base.block_id,
  rewards_event_base.block_timestamp,
  rewards_event_base.tx_id,
  rewards_event_base.msg_type,
  rewards_event_base.msg_index,
  rewards_event_base.event_rewards_amount AS amount,
  rewards_event_base.event_rewards_currency AS currency,
  'withdraw_validator_commission' AS action,
  'distribution' AS module,
  rewards.validator_address AS validator_address,
  rewards_event_base.delegator_address AS delegator_address
FROM
  rewards_event_base
  LEFT JOIN rewards
  ON rewards_event_base.tx_id = rewards.tx_id
