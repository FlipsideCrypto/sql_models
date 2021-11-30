{{ 
  config(
    materialized='table', 
    unique_key='label', 
    tags=['snowflake', 'terra', 'labels']
  )
}}

SELECT
project_name as label,
max(CASE WHEN address_name = 'operator_address' then address else NULL end) as operator_address,
max(CASE WHEN address_name = 'delegator_address' then address else NULL end) as delegator_address,
max(CASE WHEN address_name = 'vp_address' then address else NULL end) as vp_address
FROM {{ref('silver_crosschain__address_labels')}} 
WHERE blockchain = 'terra'
  AND l1_label = 'operator'
GROUP BY label