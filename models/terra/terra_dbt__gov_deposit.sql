{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = 'block_id',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp'],
  tags = ['snowflake', 'terra', 'gov']
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
    msg_value :depositor,
    '\"',
    ''
  ) AS depositor,
  REGEXP_REPLACE(
    msg_value :proposal_id,
    '\"',
    ''
  ) AS proposal_id,
  REGEXP_REPLACE(msg_value :amount [0] :amount / pow(10, 6), '\"', '') AS amount,
  REGEXP_REPLACE(
    msg_value :amount [0] :denom,
    '\"',
    ''
  ) AS currency
FROM
  {{ ref('silver_terra__msgs') }}
WHERE
  msg_module = 'gov'
  AND msg_type = 'gov/MsgDeposit'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days' -- {% else %}
--  AND block_timestamp >= getdate() - interval '9 months'
{% endif %}
