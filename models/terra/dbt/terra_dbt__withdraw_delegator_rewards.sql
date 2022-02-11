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
    amt.index AS event_attributes_amount_index,
    amt.value :amount / pow(
      10,
      6
    ) AS event_rewards_amount,
    amt.value :denom :: STRING AS event_rewards_currency,
    event_attributes :validator :: STRING AS validator_address
  FROM
    {{ ref('silver_terra__msg_events') }},
    LATERAL FLATTEN(
      input => event_attributes :amount
    ) amt
  WHERE
    msg_module = 'distribution'
    AND msg_type = 'distribution/MsgWithdrawDelegationReward'
    AND event_type = 'withdraw_rewards'
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
    msg_value :delegator_address :: STRING AS delegator_address,
    msg_value :validator_address :: STRING AS validator_address,
    REGEXP_REPLACE(msg_value :amount :amount / pow(10, 6), '\"', '') AS event_amount,
    msg_value :amount :denom :: STRING AS event_currency
  FROM
    {{ ref('silver_terra__msgs') }}
  WHERE
    msg_module = 'distribution'
    AND msg_type = 'distribution/MsgWithdrawDelegationReward'
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
    event_attributes_amount_index,
    event_rewards_amount,
    event_rewards_currency,
    validator_address
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
  rewards_event_base.event_attributes_amount_index,
  rewards_event_base.event_rewards_amount,
  rewards_event_base.event_rewards_currency,
  'withdraw_delegator_rewards' AS action,
  rewards_event_base.validator_address,
  rewards.delegator_address AS delegator_address
FROM
  rewards_event_base
  LEFT JOIN rewards
  ON rewards_event_base.tx_id = rewards.tx_id
  AND rewards_event_base.validator_address = rewards.validator_address
