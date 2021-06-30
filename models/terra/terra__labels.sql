{{ config(
      materialized='view',
      tags=['snowflake', 'terra_views', 'labels']  
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
  AND l1_label != 'cex'