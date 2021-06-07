{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'oracle']
  )
}}

SELECT * FROM {{ ref('terra_dbt__aggregate_exchange_rate_vote') }}

UNION

SELECT * FROM {{ ref('terra_dbt__exchange_rate_vote') }}