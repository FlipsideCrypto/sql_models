{{ config(
  materialized = 'incremental',
  unique_key='start_Date',
  incremental_strategy = 'delete+insert',
  cluster_by = ['start_date],
  tags = ['snowflake', 'terra_gold', 'terra_velocity']
) }}

{{ config(
  materialized='table',
  sort='start_date',
  tags=['custom'])
}}
SELECT
    blockchain,
    start_date,
    end_date,
    currency,
    metric,
    value
FROM
    {{ source('shared', 'udm_velocity')}}
WHERE
    blockchain = 'terra'