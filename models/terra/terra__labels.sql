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
  project_name as label
FROM {{ref('silver_crosschain__address_labels')}}
WHERE blockchain = 'terra'
  AND l1_label != 'cex'