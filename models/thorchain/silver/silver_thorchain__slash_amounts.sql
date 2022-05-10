{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'slash_amounts']
) }}

SELECT
  *
FROM
  {{ ref('thorchain_dbt__slash_amounts') }}
  qualify(ROW_NUMBER() over(PARTITION BY asset, block_timestamp
ORDER BY
  __HEVO__INGESTED_AT DESC)) = 1

{% if is_incremental() %}
WHERE
  __HEVO_loaded_at >= (
    SELECT
      MAX(__HEVO_loaded_at)
    FROM
      {{ this }}
  )
{% endif %}
