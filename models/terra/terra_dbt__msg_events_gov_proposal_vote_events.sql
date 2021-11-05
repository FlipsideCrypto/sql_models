{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = "block_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'gov']
) }}

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
  event_attributes :option :: STRING AS OPTION,
  event_attributes :proposal_id AS proposal_id,
  event_attributes :validator :: STRING AS validator
FROM
  {{ ref('silver_terra__msg_events') }}
WHERE
  msg_module = 'gov'
  AND event_type = 'proposal_vote'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days' -- {% else %}
--  AND block_timestamp >= getdate() - interval '9 months'
{% endif %}
