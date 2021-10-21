{{ config(
  materialized = 'incremental',
  unique_key = 'block_id || contract_address || function_name || inputs',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_id', 'contract_address', 'function_name'],
  tags = ['snowflake', 'ethereum', 'reads']
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
FROM
  {{ ref('silver_ethereum_dbt__reads') }} 

{% if is_incremental() %}
AND system_created_at :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(system_created_at :: DATE))
  FROM
    {{ this }} AS silver_ethereum__reads
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY block_id, contract_address, function_name, inputs
ORDER BY
  system_created_at DESC)) = 1