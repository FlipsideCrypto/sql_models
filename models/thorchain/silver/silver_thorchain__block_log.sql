{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'block_log']
) }}

SELECT
  *
FROM
  {{ ref('thorchain_dbt__block_log') }}
  qualify(ROW_NUMBER() over(PARTITION BY height, TIMESTAMP, HASH
ORDER BY
  __HEVO__INGESTED_AT DESC)) = 1

{% if is_incremental() %}
AND __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}
