{{ config(
      materialized='view',
      tags=['snowflake', 'polygon_views', 'labels', 'polygon_labels', 'address_labels']  
    ) 
}}

SELECT
  blockchain, 
  creator, 
  address,
  l1_label as label_type,
  l2_label as label_subtype,
  project_name as label, 
  address_name as address_name
FROM {{ref('silver_crosschain__address_labels')}}
WHERE blockchain = 'polygon'