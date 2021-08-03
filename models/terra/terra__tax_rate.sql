{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_number', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
    tags=['snowflake', 'terra', 'tax_rate']
  )
}}

SELECT
  blockchain,
  block_timestamp,
  block_number,
  tax_rate
FROM {{source('terra', 'udm_custom_fields_terra_tax_rate')}}
WHERE 1=1 
{% if is_incremental() %}
  AND block_timestamp >= getdate() - interval '1 days'
-- {% else %}
--   block_timestamp >= getdate() - interval '9 months'
{% endif %}