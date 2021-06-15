{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
    tags=['snowflake', 'terra', 'tax_rate']
  )
}}

SELECT 
  blockchain,
  block_timestamp,
  block_id,
  tax_rate
FROM {{source('terra', 'udm_custom_fields_terra_tax_rate')}}
WHERE
{% if is_incremental() %}
  block_timestamp >= getdate() - interval '1 days'
{% else %}
  block_timestamp >= getdate() - interval '9 months'
{% endif %}