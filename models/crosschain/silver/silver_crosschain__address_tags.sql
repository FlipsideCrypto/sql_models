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
  {{ source(
      'shared',
      'udm_address_tags'
    ) }}

WHERE
  1 = 1

qualify(ROW_NUMBER() over(PARTITION BY blockchain, address, tag_name
ORDER BY
  start_date DESC)) = 1
