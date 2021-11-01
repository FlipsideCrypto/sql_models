{{ config(
  materialized = 'incremental',
  unique_key = 'block_id || tx_id',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp', 'block_id'],
  tags = ['snowflake', 'terra', 'mirror', 'mirror_gov']
) }}

WITH msgs AS (

  SELECT
    blockchain,
    chain_id,
    block_id,
    block_timestamp,
    tx_id,
    msg_value :execute_msg :send :amount / pow(
      10,
      6
    ) AS amount,
    msg_value :execute_msg :send :msg :create_poll :title :: STRING AS title,
    msg_value :execute_msg :send :msg :create_poll :link :: STRING AS link,
    msg_value :execute_msg :send :msg :create_poll :description :: STRING AS description,
    coalesce(msg_value :execute_msg :send :msg :create_poll :execute_msg :msg, msg_value:execute_msg:send:msg) AS msg,
    msg_value :execute_msg :send :contract :: STRING AS contract_address
  FROM
    {{ ref('silver_terra__msgs') }}
  WHERE
    msg_value :execute_msg :send :contract :: STRING = 'terra1wh39swv7nq36pnefnupttm2nr96kz7jjddyt2x' -- MIR Governance
    AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
  SELECT
    MAX(
      block_timestamp :: DATE
    )
  FROM
    {{ ref('silver_terra__msgs') }}
)
{% endif %}
),
events AS (
  SELECT
    tx_id,
    COALESCE(TO_TIMESTAMP(event_attributes :end_time), event_attributes :end_height) AS end_time,
    event_attributes :poll_id :: NUMBER AS poll_id
  FROM
    {{ ref('silver_terra__msg_events') }}
  WHERE
    tx_id IN(
      SELECT
        tx_id
      FROM
        msgs
    )
    AND event_attributes :"0_action" :: STRING = 'send'
    AND event_type = 'from_contract'
    AND poll_id IS NOT NULL

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
  SELECT
    MAX(
      block_timestamp :: DATE
    )
  FROM
    {{ ref('silver_terra__msgs') }}
)
{% endif %}
)
SELECT
  m.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  m.tx_id,
  poll_id,
  end_time,
  amount,
  title,
  link,
  description,
  msg,
  contract_address,
  l.address AS contract_label
FROM
  msgs m
  JOIN events e
  ON m.tx_id = e.tx_id
  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels')}} AS l
  ON contract_address = l.address
