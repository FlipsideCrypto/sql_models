{{ config(
  materialized = 'incremental',
  unique_key = 'block_id || contract_address || function_name',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_id', 'contract_address', 'function_name'],
  tags = ['snowflake', 'ethereum', 'reads', 'silver_ethereum__reads']
) }}

WITH silver AS (
  SELECT
    system_created_at, 
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
  FROM
    {{ ref('ethereum_dbt__reads') }} 
  WHERE 
    1 = 1

  {% if is_incremental() %}
  AND block_timestamp :: DATE >= (
    SELECT
      DATEADD('day', -1, MAX(block_timestamp :: DATE))
    FROM
      {{ this }} AS ethereum_reads
  )
  {% endif %}

  UNION ALL 
  SELECT 
    '2000-01-01' :: TIMESTAMP AS system_created_at, 
    block_timestamp, 
    block_id, 
    contract_address,
    contract_name,
    function_name,
    inputs,
    project_id,
    project_name,
    value_numeric,
    value_str
  FROM {{ source (
    'ethereum', 
    'ethereum_reads'
  ) }}
  WHERE 
    1 = 1

  {% if is_incremental() %}
  AND block_timestamp :: DATE >= (
    SELECT
      DATEADD('day', -1, MAX(block_timestamp :: DATE))
    FROM
      {{ this }} AS ethereum_reads
  )
  {% endif %}
  )

  SELECT 
    system_created_at, 
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
  FROM 
    silver qualify(ROW_NUMBER() over(PARTITION BY block_id, contract_address, function_name, inputs
  ORDER BY
    system_created_at DESC)) = 1