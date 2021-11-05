{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = "block_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'transfer']
) }}

WITH input AS (

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
    key,
    SPLIT(
      key,
      '_'
    ) [0] :: STRING AS message_id,
    SPLIT(
      key,
      '_'
    ) [1] :: STRING AS message_attribute,
    VALUE
  FROM
    {{ ref('silver_terra__msg_events') }},
    LATERAL FLATTEN(
      input => event_attributes
    ) vm
  WHERE
    msg_module = 'bank'
    AND msg_type = 'bank/MsgMultiSend'
    AND event_type = 'transfer'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days' -- {% else %}
--  AND block_timestamp >= getdate() - interval '9 months'
{% endif %}
),
tbl_recipient AS (
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
    VALUE AS recipient,
    message_id
  FROM
    input
  WHERE
    message_attribute = 'recipient'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days' -- {% else %}
--  AND block_timestamp >= getdate() - interval '9 months'
{% endif %}
),
tbl_amount AS (
  SELECT
    tx_id,
    message_id,
    VALUE :amount AS amount,
    VALUE :denom :: STRING AS currency
  FROM
    input
  WHERE
    message_attribute = 'amount'
)
SELECT
  blockchain,
  chain_id,
  tx_status,
  block_id,
  block_timestamp,
  tbl_recipient.tx_id,
  tx_type,
  msg_module,
  msg_type,
  event_type,
  event_attributes,
  recipient,
  tbl_amount.amount,
  tbl_amount.currency,
  tbl_recipient.message_id
FROM
  tbl_recipient
  LEFT JOIN tbl_amount
  ON tbl_recipient.tx_id = tbl_amount.tx_id
  AND tbl_recipient.message_id = tbl_amount.message_id
