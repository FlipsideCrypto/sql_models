{{ config(
  materialized = 'incremental',
  unique_key = 'block_id || tx_hash || log_index || to',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp'],
  tags = ['snowflake', 'ethereum', 'silver_ethereum','silver_ethereum__events']
) }}


select *
from (
SELECT
  system_created_at,
  block_id,
  block_timestamp,
  tx_hash,
  input_method,
  "from",
  "to",
  name,
  symbol,
  contract_address,
  eth_value,
  fee,
  log_index,
  log_method,
  token_value
FROM
  {{ ref('ethereum_dbt__events') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(block_timestamp :: DATE))
  FROM
    {{ this }} AS events
)
{% endif %}

union

SELECT
  '2000-01-01'::timestamp as system_created_at,
  block_id,
  block_timestamp,
  tx_hash,
  input_method,
  "from",
  "to",
  name,
  symbol,
  contract_address,
  eth_value,
  fee,
  log_index,
  log_method,
  token_value
FROM
  {{ source('ethereum','ethereum_events') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(block_timestamp :: DATE))
  FROM
    {{ this }} AS events
)
{% endif %}

QUALIFY(rank() over(partition by tx_hash order by block_id desc)) = 1
) a
qualify(ROW_NUMBER() over(PARTITION BY block_id, tx_hash, log_index, "to"
ORDER BY
  system_created_at DESC)) = 1
