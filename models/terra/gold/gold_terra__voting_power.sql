{{ config(
  materialized = 'incremental',
  unique_key = 'blockchain || address',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_id', 'block_timestamp'],
  tags = ['snowflake', 'terra_gold', 'terra_voting_power']
) }}

SELECT
  blockchain,
  block_timestamp,
  block_number,
  address,
  voting_power
FROM
  {{source('terra','terra_validator_voting_power')}}
WHERE
  {% if is_incremental() %}
    block_timestamp >= getdate() - interval '3 days'
  {% endif %}