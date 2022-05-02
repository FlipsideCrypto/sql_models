{{ config(
  materialized = 'incremental',
  unique_key = "_inserted_timestamp::date",
  incremental_strategy = 'delete+insert',
  cluster_by = ['_inserted_timestamp::DATE'],
  tags = ['snowflake', 'terra_silver', 'terra_balances']
) }}

WITH base_tables AS (

  SELECT
    *
  FROM
    {{ source(
      'bronze',
      'prod_terra_sink_645110886'
    ) }}
  WHERE
    record_content :model :class :: STRING IN (
      'terra.balances.models.Terra5BalancesModel',
      'terra.balances.models.Terra5DelegationsModel',
      'terra.balances.models.TerraBalancesModel',
      'terra.balances.models.TerraDelegationsModel'
    )

{% if is_incremental() %}
  AND _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp)
    FROM
      {{ source(
      'bronze',
      'prod_terra_sink_645110886'
      ) }}
  )
{% endif %}
)

SELECT
  (
    record_metadata :CreateTime :: INT / 1000
  ) :: TIMESTAMP AS system_created_at,
  _inserted_timestamp,
  t.value :address :: STRING AS address,
  t.value :balance :: FLOAT AS balance,
  t.value :balance_type :: STRING AS balance_type,
  t.value :block_id :: INT AS block_number,
  t.value :block_timestamp :: TIMESTAMP AS block_timestamp,
  t.value :blockchain :: STRING AS blockchain,
  t.value :currency :: STRING AS currency
FROM
  base_tables,
  LATERAL FLATTEN(
    input => record_content :results
  ) t