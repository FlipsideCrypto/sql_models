{{ config(
  materialized = 'incremental',
  unique_key = 'blockchain || address || creator',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'crosschain', 'silver_dbt__address_tags']
) }}

WITH base_tables AS (

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

{% if is_incremental() %}
AND (
  record_metadata :CreateTime :: INT / 1000
) :: TIMESTAMP :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(system_created_at :: DATE))
  FROM
    {{ this }}
)
{% endif %}
)
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
  base_tables
