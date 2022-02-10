{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = "block_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'voting_power']
) }}

SELECT
  block_number AS block_id,
  block_timestamp,
  blockchain,
  address,
  voting_power
FROM
  {{ source(
    'terra',
    'terra_validator_voting_power'
  ) }}

{% if is_incremental() %}
WHERE
  block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_id, address
ORDER BY
  block_timestamp DESC)) = 1
