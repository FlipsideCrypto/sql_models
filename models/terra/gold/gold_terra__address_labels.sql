{{ 
  config(
    materialized='incremental',
    unique_key='blockchain || address', 
    incremental_strategy='delete+insert',
    cluster_by=['block_id, 'block_timestamp'],
    tags=['snowflake', 'terra_gold', 'terra_address_labels']
  )
}}

SELECT
    *
FROM
    {{ source(
        'shared',
        'udm_address_labels_new'
    ) }}
WHERE
    blockchain = 'terra'
