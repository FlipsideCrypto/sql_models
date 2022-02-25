{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', address, block_number)",
  incremental_strategy = 'delete+insert',
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
    record_content :model :name :: STRING IN (
      'terra_balances', 
      'terra-5_balances',
      'terra_delegations', 
      'terra-5_delegations', 
      'terra-5_delegations_bison', 
      'terra-5_balances_bison'
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
),

terra_balance_dbt_table AS (
  SELECT
    t.value :address :: STRING AS address,
    t.value :balance :: INT AS balance,
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
),

terra_balance_columbus_3_table AS (
  SELECT 
    address,
    balance,
    balance_type,
    block_number,
    block_timestamp,
    'columbus-3' AS blockchain,
    currency
  FROM {{ source('shared', 'terra_balances') }}
  WHERE date(block_timestamp) < '2020-10-04' AND block_number <= 3820000
)

SELECT
  address,
  balance,
  balance_type,
  block_number,
  block_timestamp,
  blockchain,
  currency
FROM terra_balance_dbt_table

UNION ALL 

SELECT
  address,
  balance,
  balance_type,
  block_number,
  block_timestamp,
  blockchain,
  currency
FROM terra_balance_columbus_3_table