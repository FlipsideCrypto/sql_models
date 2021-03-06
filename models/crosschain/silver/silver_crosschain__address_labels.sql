{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', blockchain, address, creator)",
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
