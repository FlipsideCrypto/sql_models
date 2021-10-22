{{ config(
  materialized = 'incremental',
  unique_key = 'block_id || tx_hash',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp'],
  tags = ['snowflake', 'ethereum', 'silver_ethereum','silver_ethereum__transactions']
) }}


SELECT
  system_created_at,
  block_id,
  block_timestamp,
  tx_hash,
  tx_position,
  nonce,
  from_address,
  to_address,
  input_method,
  gas_price,
  gas_limit,
  gas_used,
  success
FROM
  {{ ref('ethereum_dbt__transactions') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(block_timestamp :: DATE))
  FROM
    {{ this }} AS transactions
)
{% endif %}

union

SELECT
  '2000-01-01'::timestamp as system_created_at,
  block_id,
  block_timestamp,
  tx_hash,
  tx_position,
  nonce,
  from_address,
  to_address,
  input_method,
  gas_price,
  gas_limit,
  gas_used,
  success
FROM
  {{ source('ethereum','ethereum_transactions') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(block_timestamp :: DATE))
  FROM
    {{ this }} AS transactions
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY tx_hash
ORDER BY
    block_id, 
    system_created_at DESC)) = 1