{{ config(
      materialized='table',
      sort='address',
      tags=['snowflake', 'terra', 'labels']  
    ) 
}}

SELECT
  blockchain, 
  address,
  l1_label as label_type,
  l2_label as label_subtype,
  project_name as label,
  address_name
FROM {{source('shared', 'udm_address_labels_new')}}
WHERE blockchain = 'terra'
  AND project_name IN('terraswap', 'chai', 
                      'mirror', 'anchor')
