{{ config(
  materialized = 'incremental',
  unique_key = 'blockchain || address || tag_name',
  incremental_strategy = 'delete+insert',
  cluster_by = ['blockchain', 'address'],
  tags = ['snowflake', 'crosschain', 'silver_crosschain__address_tags']
) }}

SELECT
  blockchain,
  address,
  tag_name,
  tag_type,
  tag_slug,
  source, 
  start_date, 
  end_date
FROM
  {{ ref('silver_dbt__address_tags') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND system_created_at :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(system_created_at :: DATE))
  FROM
    {{ this }} AS address_tags
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY blockchain, address, tag_name
ORDER BY
  start_date DESC)) = 1
