{{ config(
      materialized='table',
      sort='address',
      tags=['snowflake', 'terra', 'labels']  
    ) 
}}

SELECT
  blockchain, 
  address,
  l1_label,
  l2_label,
  project_name,
  address_name
FROM {{source('shared', 'udm_address_labels_new')}}
WHERE blockchain = 'terra'
  AND project_name IN('terraswap', 'chai', 
                      'mirror', 'anchor')
