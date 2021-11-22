{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = "block_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'staking']
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
  msg_index,
  event_type,
  event_attributes,
  event_attributes :validator :: STRING AS validator,
  CASE
    WHEN event_type = 'delegate' THEN event_attributes :amount
    ELSE NULL
  END AS delegated_amount,
  event_attributes :"0_sender" :: STRING AS "0_sender",
  event_attributes :"1_sender" :: STRING AS "1_sender",
  event_attributes :action :: STRING AS action,
  event_attributes :module :: STRING AS module,
  CASE
    WHEN event_type = 'transfer' THEN event_attributes :amount :amount
    ELSE NULL
  END AS event_transfer_amount,
  CASE
    WHEN event_type = 'transfer' THEN event_attributes :amount :denom :: STRING
    ELSE NULL
  END AS event_transfer_currency,
  event_attributes :sender :: STRING AS sender,
  event_attributes :recipient :: STRING AS recipient
FROM
  {{ ref('silver_terra__msg_events') }}
WHERE
  msg_module = 'staking'
  AND msg_type = 'staking/MsgDelegate'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
