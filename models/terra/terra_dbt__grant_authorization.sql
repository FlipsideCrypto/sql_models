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
    msg_value :authorization :type,
    '\"',
    ''
  ) AS TYPE,
  REGEXP_REPLACE(
    msg_value :authorization :value :spend_limit [0] :amount,
    '\"',
    ''
  ) AS amount,
  REGEXP_REPLACE(
    msg_value :authorization :value :spend_limit [0] :denom,
    '\"',
    ''
  ) AS currency,
  REGEXP_REPLACE(
    msg_value :grantee,
    '\"',
    ''
  ) AS grantee,
  REGEXP_REPLACE(
    msg_value :granter,
    '\"',
    ''
  ) AS granter,
  REGEXP_REPLACE(
    msg_value :period,
    '\"',
    ''
  ) AS period
FROM
  {{ ref('silver_terra__msgs') }}
WHERE
  msg_module = 'msgauth'
  AND msg_type = 'msgauth/MsgGrantAuthorization'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days' -- {% else %}
--  AND block_timestamp >= getdate() - interval '9 months'
{% endif %}
