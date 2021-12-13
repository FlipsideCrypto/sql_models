{{ config(
  materialized = 'incremental',
  unique_key = 'blockchain || address || creator',
  incremental_strategy = 'delete+insert',
  cluster_by = ['blockchain', 'address'],
  tags = ['snowflake', 'crosschain', 'address_labels', 'silver_crosschain__address_labels']
) }}

SELECT
  system_created_at,
  insert_date,
  blockchain,
  address,
  creator,
  l1_label,
  l2_label,
  address_name, 
  project_name
FROM
  {{ ref('silver_dbt__address_labels') }}
WHERE blockchain <> 'ethereum' OR (blockchain = 'ethereum' AND insert_date > '2021-11-01' AND (address LIKE '0x' OR address LIKE '0X'))

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
  insert_date DESC)) = 1
