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