{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = "block_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'grant']
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
  event_attributes :grant_type :: STRING AS grant_type,
  event_attributes :grantee :: STRING AS grantee,
  event_attributes :granter :: STRING AS granter
FROM
  {{ ref('silver_terra__msg_events') }}
WHERE
  msg_module = 'msgauth'
  AND event_type = 'grant_authorization'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
