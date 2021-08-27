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
  tax_rate
FROM
  {{source('terra','udm_custom_fields_terra_tax_rate')}}
WHERE
{% if is_incremental() %}
  block_timestamp >= getdate() - interval '3 days'
{% else %}
  block_timestamp >= getdate() - interval '12 months'
{% endif %}