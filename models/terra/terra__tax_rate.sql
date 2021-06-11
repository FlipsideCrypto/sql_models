{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'tax_rate']
  )
}}

SELECT 
  blockchain,
  block_timestamp,
  block_id,
  tax_rate
FROM {{source('terra', 'udm_custom_fields_terra_tax_rate')}}
{% if is_incremental() %}
 AND block_timestamp >= getdate() - interval '1 days'
{% else %}
 AND block_timestamp >= getdate() - interval '9 months'
{% endif %}