{{ config(
  materialized = 'incremental',
  unique_key = 'block_id || tx_id || event_index',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp'],
  tags = ['snowflake', 'ethereum', 'silver_ethereum','silver_ethereum__events_emitted']
) }}


select *
from (
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

union

SELECT
  '2000-01-01'::timestamp as system_created_at,
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
  {{ source('ethereum','ethereum_events_emitted') }}
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

QUALIFY(rank() over(partition by tx_id order by block_id desc)) = 1
) a
qualify(ROW_NUMBER() over(PARTITION BY block_id, tx_id, event_index
ORDER BY
  system_created_at DESC)) = 1
