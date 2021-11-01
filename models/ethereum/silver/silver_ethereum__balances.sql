{{ config(
  materialized = 'incremental',
  unique_key = 'address || block_id || contract_address',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp'],
  tags = ['snowflake', 'ethereum', 'silver_ethereum','silver_ethereum__balances']
) }}

select system_created_at,
  block_id,
  block_timestamp,
  address,
  balance,
  contract_address,
  project_id,
  project_name
from (
SELECT
  system_created_at,
  block_id,
  block_timestamp,
  address,
  balance,
  contract_address,
  project_id,
  project_name
FROM
  {{ ref('ethereum_dbt__balances') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(block_timestamp :: DATE))
  FROM
    {{ this }} AS balances
)
{% endif %}

union

SELECT
  '2000-01-01'::timestamp as system_created_at,
  block_id,
  block_timestamp,
  address,
  balance,
  contract_address,
  project_id,
  project_name
FROM
  {{ source('ethereum','ethereum_balances') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(block_timestamp :: DATE))
  FROM
    {{ this }} AS balances
)
{% endif %}
)
qualify(ROW_NUMBER() over(PARTITION BY address, block_id, contract_address
ORDER BY
  system_created_at DESC)) = 1
