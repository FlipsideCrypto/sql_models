{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = "block_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'oracles']
) }}

SELECT
  blockchain,
  chain_id,
  tx_status,
  block_id,
  block_timestamp,
  tx_id,
  msg_type,
  REGEXP_REPLACE(
    msg_value :delegate,
    '\"',
    ''
  ) AS delegator,
  REGEXP_REPLACE(
    msg_value :operator,
    '\"',
    ''
  ) AS validator,
  msg_value
FROM
  {{ ref('silver_terra__msgs') }}
WHERE
  msg_module = 'oracle'
  AND msg_type = 'oracle/MsgDelegateFeedConsent'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
