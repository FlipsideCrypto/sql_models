{{ config(
  materialized = 'incremental',
  unique_key = 'block_id || tx_id || event_index',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp'],
  tags = ['snowflake', 'ethereum', 'silver_ethereum','silver_ethereum__events_emitted', 'ab_test']
) }}

SELECT
  *
FROM
  (
    SELECT
      system_created_at,
      block_id,
      block_timestamp,
      contract_addr,
      contract_name,
      event_index,
      event_inputs,
      event_name,
      event_removed,
      tx_from_addr,
      tx_id,
      tx_succeeded,
      tx_to_addr
    FROM
      {{ ref('ethereum_dbt__events_emitted') }}
    WHERE
      1 = 1

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(block_timestamp :: DATE))
  FROM
    {{ this }} AS events_emitted
)
{% endif %}
UNION
SELECT
  '2000-01-01' :: TIMESTAMP AS system_created_at,
  block_id,
  block_timestamp,
  contract_addr,
  contract_name,
  event_index,
  event_inputs,
  event_name,
  event_removed,
  tx_from_addr,
  tx_id,
  tx_succeeded,
  tx_to_addr
FROM
  {{ source(
    'ethereum',
    'ethereum_events_emitted'
  ) }}
WHERE
  block_timestamp :: DATE < '2020-01-13'
  AND 1 = 1

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(block_timestamp :: DATE))
  FROM
    {{ this }} AS events_emitted
)
{% endif %}

qualify(RANK() over(PARTITION BY tx_id
ORDER BY
  block_id DESC)) = 1
) A qualify(ROW_NUMBER() over(PARTITION BY block_id, tx_id, event_index
ORDER BY
  system_created_at DESC)) = 1
