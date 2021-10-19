{{ config(
  materialized = 'incremental',
  unique_key = 'tx_id || nf_token_id',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'uniswapv3_silver', 'uniswapv3_dbt__pools']
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
      'uniswap_v3_mainnet_pool_model'
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
  ------- replicate scheme
FROM
  base_tables,
  LATERAL FLATTEN(
    input => record_content :results
  ) t
