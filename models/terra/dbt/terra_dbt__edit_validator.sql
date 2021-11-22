{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = "block_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'staking']
) }}

WITH staking_events AS (

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
    event_attributes :action :: STRING AS action,
    event_attributes :sender :: STRING AS sender,
    event_attributes :module :: STRING AS module,
    event_attributes :commission_rate :: STRING AS commission_rate_attribute,
    SPLIT(
      SPLIT(
        event_attributes :commission_rate :: STRING,
        ','
      ) [0],
      ': '
    ) [1] :: FLOAT AS commission_rate,
    SPLIT(
      SPLIT(
        event_attributes :commission_rate :: STRING,
        ','
      ) [1],
      ': '
    ) [1] :: FLOAT AS commission_maxRate,
    SPLIT(
      SPLIT(
        event_attributes :commission_rate :: STRING,
        ','
      ) [2],
      ': '
    ) [1] :: FLOAT AS commission_maxChangeRate,
    SPLIT(
      SPLIT(
        event_attributes :commission_rate :: STRING,
        ','
      ) [3],
      ': '
    ) [1] :: STRING AS commission_updateTime,
    event_attributes :min_self_delegation AS min_self_delegation
  FROM
    {{ ref('silver_terra__msg_events') }}
  WHERE
    msg_module = 'staking'
    AND msg_type = 'staking/MsgEditValidator'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
),
staking AS (
  SELECT
    blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp,
    tx_id,
    msg_type,
    REGEXP_REPLACE(
      msg_value :address,
      '\"',
      ''
    ) AS validator,
    REGEXP_REPLACE(
      msg_value :commission_rate,
      '\"',
      ''
    ) AS commission_rate,
    REGEXP_REPLACE(
      msg_value :Description :details,
      '\"',
      ''
    ) AS details,
    REGEXP_REPLACE(
      msg_value :Description :identity,
      '\"',
      ''
    ) AS identity,
    REGEXP_REPLACE(
      msg_value :Description :moniker,
      '\"',
      ''
    ) AS moniker,
    REGEXP_REPLACE(
      msg_value :Description :security_contact,
      '\"',
      ''
    ) AS security_contact,
    REGEXP_REPLACE(
      msg_value :Description :website,
      '\"',
      ''
    ) AS website,
    NULL AS msg_value
  FROM
    {{ ref('silver_terra__msgs') }}
  WHERE
    msg_module = 'staking'
    AND msg_type = 'staking/MsgEditValidator'
    AND chain_id = 'columbus-3'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
UNION
SELECT
  blockchain,
  chain_id,
  tx_status,
  block_id,
  block_timestamp,
  tx_id,
  msg_type,
  REGEXP_REPLACE(
    msg_value :address,
    '\"',
    ''
  ) AS validator,
  REGEXP_REPLACE(
    msg_value :commission_rate,
    '\"',
    ''
  ) AS commission_rate,
  REGEXP_REPLACE(
    msg_value :description :details,
    '\"',
    ''
  ) AS details,
  REGEXP_REPLACE(
    msg_value :description :identity,
    '\"',
    ''
  ) AS identity,
  REGEXP_REPLACE(
    msg_value :description :moniker,
    '\"',
    ''
  ) AS moniker,
  REGEXP_REPLACE(
    msg_value :description :security_contact,
    '\"',
    ''
  ) AS security_contact,
  REGEXP_REPLACE(
    msg_value :description :website,
    '\"',
    ''
  ) AS website,
  msg_value
FROM
  {{ ref('silver_terra__msgs') }}
WHERE
  msg_module = 'staking'
  AND msg_type = 'staking/MsgEditValidator'
  AND chain_id = 'columbus-4'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
),
event_base AS (
  SELECT
    DISTINCT blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp,
    tx_id,
    msg_type
  FROM
    staking
),
message AS (
  SELECT
    tx_id,
    action,
    module,
    sender
  FROM
    staking_events
  WHERE
    event_type = 'message'
),
edit_validator AS (
  SELECT
    tx_id,
    commission_rate,
    commission_maxRate,
    commission_maxChangeRate,
    commission_updateTime
  FROM
    staking_events
  WHERE
    event_type = 'edit_validator'
)
SELECT
  event_base.blockchain,
  event_base.chain_id,
  event_base.tx_status,
  event_base.block_id,
  event_base.block_timestamp,
  event_base.tx_id,
  event_base.msg_type,
  action,
  module,
  sender,
  -- edit_validator.commission_rate,
  commission_maxRate,
  commission_maxChangeRate,
  commission_updateTime,
  staking.validator,
  staking.commission_rate,
  staking.details,
  staking.identity,
  staking.moniker,
  staking.security_contact,
  staking.website
FROM
  event_base
  LEFT JOIN message
  ON event_base.tx_id = message.tx_id
  LEFT JOIN edit_validator
  ON event_base.tx_id = edit_validator.tx_id
  LEFT JOIN staking
  ON event_base.tx_id = staking.tx_id
