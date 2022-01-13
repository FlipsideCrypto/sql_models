{{ config(
  materialized = 'incremental',
  unique_key = 'address',
  incremental_strategy = 'delete+insert',
  cluster_by = ['address'],
  tags = ['snowflake', 'ethereum', 'silver_ethereum','silver_ethereum__contracts']
) }}

select system_created_at,
  address,
  meta,
  name
from 
(
SELECT
  system_created_at,
  address,
  meta,
  name
FROM
  {{ ref('ethereum_dbt__contracts') }}

union

SELECT
  '2000-01-01'::timestamp as system_created_at,
  address,
  meta,
  name
FROM
  {{ source('ethereum','ethereum_contracts') }}

union

select 
  '2000-01-01'::timestamp as system_created_at,
  contract_address as address,
  to_object(parse_json(contract_meta)) as meta,
  name
from {{ source('ethereum','ethereum_contracts_backfill') }}
where check_json(contract_meta) is null
)
where address is not null
qualify(ROW_NUMBER() over(PARTITION BY address
ORDER BY
  system_created_at DESC)) = 1

