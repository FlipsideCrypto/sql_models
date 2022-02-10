{{ config(
  materialized = 'view',
  tags = ['snowflake', 'terra_views', 'transfer_events', 'terra']
) }}

SELECT
  block_timestamp,
  tx_id,
  msg_index,
  action_index,
  initial_action,
  current_action,
  sender,
  recipient,
  amount,
  denom as currency,
  msg_sender,
  contract_address
FROM
  {{ ref('silver_terra__transfer_events') }}
