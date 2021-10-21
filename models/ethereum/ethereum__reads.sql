{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp', 'contract_address'],
    tags=['snowflake', 'ethereum', 'reads']
  )
}}

SELECT 
  BLOCK_ID AS block_id,
  BLOCK_TIMESTAMP AS block_timestamp,
  CONTRACT_ADDRESS AS contract_address,
  COALESCE(contract_labels.address_name,CONTRACT_NAME) AS contract_name,
  FUNCTION_NAME AS function_name,
  INPUTS AS inputs,
  PROJECT_ID AS project_id,
  b.PROJECT_NAME AS project_name,
  VALUE_NUMERIC AS value_numeric,
  VALUE_STR AS value_string
FROM {{ source('ethereum', 'silver_ethereum__reads') }} b

LEFT OUTER JOIN {{ source('ethereum', 'ethereum_address_labels') }} as contract_labels
 ON b.CONTRACT_ADDRESS = contract_labels.address

WHERE
{% if is_incremental() %}
  b.block_timestamp >= getdate() - interval '40 hours'
{% else %}
  b.block_timestamp >= getdate() - interval '9 months'
{% endif %}