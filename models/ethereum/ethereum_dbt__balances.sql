{{ config(
  materialized = 'incremental',
  unique_key = 'address || block_id || contract_address',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'silver_ethereum', 'ethereum_dbt__balances']
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
    record_content :model :name :: STRING IN (
      'eth_balances_model'
    )

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
  t.value :block_id :: bigint AS block_id,
  t.value :block_timestamp :: TIMESTAMP AS block_timestamp,
  t.value :address :: STRING AS address,
  t.value :balance :: FLOAT AS balance,
  t.value :contract_address :: STRING AS contract_address,
  t.value :project_id :: STRING AS project_id,
  t.value :project_name :: STRING AS project_name
FROM
  base_tables,
  LATERAL FLATTEN(
    input => record_content :results
  ) t