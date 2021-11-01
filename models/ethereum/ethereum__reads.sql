 
  {{ config(
  materialized = 'view',
  tags = ['snowflake', 'ethereum', 'reads', 'ethereum__reads']
) }}

SELECT 
    block_timestamp,
    block_id,
    contract_address,
    contract_name,
    function_name,
    inputs,
    project_id,
    project_name,
    value_numeric,
    value_string
FROM {{ ref('silver_ethereum__reads') }} 
