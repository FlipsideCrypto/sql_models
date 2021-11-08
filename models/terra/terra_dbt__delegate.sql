{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = "block_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'staking']
) }}

WITH staking AS (

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
    REGEXP_REPLACE(msg_value :amount :amount / pow(10, 6), '\"', '') AS event_amount,
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
    AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
)
SELECT
  blockchain,
  chain_id,
  tx_status,
  block_id,
  block_timestamp,
  tx_id,
  msg_type,
  delegator_address,
  validator_address,
  event_amount,
  event_currency
FROM
  staking
