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
  msg_type,
  REGEXP_REPLACE(
    msg_value :grantee,
    '\"',
    ''
  ) AS grantee,
  REGEXP_REPLACE(
    msg_value :msgs [0] :type,
    '\"',
    ''
  ) AS TYPE,
  REGEXP_REPLACE(
    msg_value :msgs [0] :value :amount [0] :amount,
    '\"',
    ''
  ) AS amount,
  REGEXP_REPLACE(
    msg_value :msgs [0] :value :amount [0] :denom,
    '\"',
    ''
  ) AS currency,
  REGEXP_REPLACE(
    msg_value :msgs [0] :value :from_address,
    '\"',
    ''
  ) AS event_from,
  REGEXP_REPLACE(
    msg_value :msgs [0] :value :to_address,
    '\"',
    ''
  ) AS event_to
FROM
  {{ ref('silver_terra__msgs') }}
WHERE
  msg_module = 'msgauth'
  AND msg_type = 'msgauth/MsgExecAuthorized'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
