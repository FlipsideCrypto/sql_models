{{ config(
  materialized = 'incremental',
  unique_key='blockchain || block_number',
  incremental_strategy = 'delete+insert',
  cluster_by = ['blockchain, 'block_number'],
  tags = ['snowflake', 'terra_gold', 'terra_tax_rate']
) }}

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
{% endif %}