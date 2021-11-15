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
    vm.value :denom,
    '\"',
    ''
  ) AS currency,
  vm.value :rate AS rate,
  REGEXP_REPLACE(
    msg_value :feeder,
    '\"',
    ''
  ) AS feeder,
  REGEXP_REPLACE(
    msg_value :salt,
    '\"',
    ''
  ) AS salt,
  REGEXP_REPLACE(
    msg_value :validator,
    '\"',
    ''
  ) AS validator
FROM
  {{ ref('silver_terra__msgs') }},
  LATERAL FLATTEN(
    input => msg_value :exchange_rates
  ) vm
WHERE
  msg_module = 'oracle'
  AND msg_type = 'oracle/MsgAggregateExchangeRateVote'

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
    msg_value :denom,
    '\"',
    ''
  ) AS currency,
  msg_value :exchange_rate AS rate,
  REGEXP_REPLACE(
    msg_value :feeder,
    '\"',
    ''
  ) AS feeder,
  REGEXP_REPLACE(
    msg_value :salt,
    '\"',
    ''
  ) AS salt,
  REGEXP_REPLACE(
    msg_value :validator,
    '\"',
    ''
  ) AS validator
FROM
  {{ ref('silver_terra__msgs') }}
WHERE
  msg_module = 'oracle'
  AND msg_type = 'oracle/MsgExchangeRateVote'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
