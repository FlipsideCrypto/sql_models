{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', chain_id, block_id, tx_id, msg_index)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra_silver', 'terra_msgs'],
  post_hook = 'delete from {{ this }} m using ( select distinct block_id, tx_id from {{ this }} where block_timestamp::date >= current_date - 7 qualify(rank() over (partition by tx_id order by block_id desc)) > 1 ) ub where m.block_id = ub.block_id and m.tx_id = ub.tx_id and m.block_timestamp::date >= current_date - 7;'
) }}

WITH msgs_uncle_blocks_removed AS (

  SELECT
    *
  FROM
    {{ ref('terra_dbt__msgs') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp)
    FROM
      {{ this }}
  )
{% endif %}

qualify(RANK() over(PARTITION BY tx_id
ORDER BY
  block_id DESC)) = 1
)
SELECT
  *
FROM
  msgs_uncle_blocks_removed qualify(ROW_NUMBER() over(PARTITION BY chain_id, block_id, tx_id, msg_index
ORDER BY
  system_created_at DESC)) = 1
