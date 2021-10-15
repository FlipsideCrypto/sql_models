{{ config(
  materialized = 'incremental',
  unique_key = 'blockchain || address || creator',
  incremental_strategy = 'delete+insert',
  cluster_by = ['blockchain', 'address'],
  tags = ['snowflake', 'crosschain', 'silver_crosschain__address_labels']
) }}

SELECT
  system_created_at,
  insert_date,
  blockchain,
  address,
  creator,
  l1_label,
  l2_label,
  project_name
FROM
  {{ ref('silver_dbt__address_labels') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND system_created_at :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(system_created_at :: DATE))
  FROM
    {{ this }} AS address_labels
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY blockchain, address, creator
ORDER BY
  system_created_at DESC)) = 1
