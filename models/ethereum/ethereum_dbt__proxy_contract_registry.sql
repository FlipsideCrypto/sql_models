{{ config(
  materialized = 'incremental',
  unique_key = 'tx_id',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'silver_ethereum', 'ethereum_dbt__proxy_contract_registry']
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
      'eth_proxy_detector_model'
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
  t.value :block_id :: INTEGER AS block_id,
  t.value :block_timestamp :: TIMESTAMP AS block_timestamp,
  t.value :logic_contract_address :: STRING AS logic_contract_address,
  t.value :proxy_contract_address :: STRING AS proxy_contract_address,
  t.value :tx_id :: STRING AS tx_id
FROM
  base_tables,
  LATERAL FLATTEN(
    input => record_content :results
  ) t