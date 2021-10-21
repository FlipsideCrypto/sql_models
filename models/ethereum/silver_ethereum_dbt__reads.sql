{{ config(
  materialized = 'incremental',
  unique_key = 'block_id || contract_address || function_name || inputs',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'ethereum', 'reads']
) }}

WITH base_tables AS (

  SELECT
    * 

  FROM
    {{ source(
      'bronze',
      'prod_ethereum_sink_407559501' 
    ) }}
  WHERE
    split(record_content:model:sinks[0]:destination::string,'.')[2]::string = 'ethereum_reads'

{% if is_incremental() %}
AND (
  record_metadata :CreateTime :: INT / 1000
) :: TIMESTAMP :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(system_created_at :: DATE))
  FROM
    {{ this }}
)
{% endif %}
)
SELECT
  (
    record_metadata :CreateTime :: INT / 1000
  ) :: TIMESTAMP AS system_created_at,
  t.value :block_timestamp :: TIMESTAMP AS block_timestamp,
  t.value :block_id :: bigint AS block_id,
  t.value :contract_address :: STRING AS contract_address,
  t.value :contract_name :: STRING AS contract_name,
  t.value :function_name :: STRING AS function_name,
  f.value ::string AS inputs,
  t.value :project_id :: STRING AS project_id,
  t.value :project_name :: STRING AS project_name,
  t.value :value_numeric :: FLOAT AS value_numeric,
  t.value :value_string :: STRING AS value_string
FROM
  base_tables,
  LATERAL FLATTEN(
    input => record_content :results
  ) t,
  LATERAL FLATTEN(parse_json(t.value :inputs)) f
