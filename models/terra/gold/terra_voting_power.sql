{{ config(
  materialized='incremental',
  sort='block_timestamp',
  unique_key='blockchain || block_number',
  tags=['custom'])
}}

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
  {% else %}
    block_timestamp >= getdate() - interval '12 months'
  {% endif %}