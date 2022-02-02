{{ config(
  materialized = 'incremental',
  unique_key = 'address',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'silver_ethereum', 'ethereum_dbt__contracts']
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
      'eth_contracts_model'
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
  coalesce(t.value :contract_address :: STRING, t.value :address :: STRING) AS address,
  coalesce(t.value :contract_meta :: OBJECT, t.value :meta :: OBJECT) AS meta,
  t.value :name :: STRING AS name
FROM
  base_tables,
  LATERAL FLATTEN(
    input => record_content :results
  ) t