{{ config(
  materialized = 'view',
  tags = ['snowflake', 'terra_views', 'event_actions', 'terra']
) }}

SELECT
  block_id,
  block_timestamp,
  a.blockchain,
  chain_id,
  tx_id,
  action_index,
  msg_index,
  action_contract_address,
  l.address_name AS contract_label,
  action_method,
  action_log
FROM
  {{ ref('silver_terra__event_actions') }} a
LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
  ON action_contract_address = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'