{{ config(
  materialized = 'incremental',
  unique_key = "decoded_contract",
  incremental_strategy = 'merge',
  tags = ['snowflake', 'terra', 'decoded', 'terra_contracts']
) }}

SELECT
v.value :contract_info :address::STRING AS decoded_contract
FROM
    {{ source(
      'bronze',
      'prod_terra_api'
    ) }}
, lateral flatten ( data :response_data) v