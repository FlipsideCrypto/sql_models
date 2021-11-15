{{ config(
  materialized = 'incremental',
  unique_key = 'blockchain || address || creator',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'crosschain', 'silver_dbt__address_labels']
) }}

WITH base_tables AS (

  SELECT
    *, 
   split(substr(record_metadata:key::string, 2, len(record_metadata:key::string)-2),'-')[1]::string as blockchain,
    to_timestamp(split(substr(record_metadata:key::string, 2, len(record_metadata:key::string)-2),'-')[2]::int) as insert_date 

  FROM
    {{ source(
      'bronze',
      'prod_address_label_sink_291098491'
    ) }}
  WHERE
    array_size(split(substr(record_metadata:key::string, 2, len(record_metadata:key::string)-2),'-')) = 3
    AND split(substr(record_metadata:key::string, 2, len(record_metadata:key::string)-2),'-')[0] = 'labels'

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
  (
    record_metadata :CreateTime :: INT / 1000
  ) :: TIMESTAMP AS system_created_at,
  insert_date,
  blockchain,
  LOWER(last_value(t.value :address :: STRING) OVER (PARTITION BY t.value :address, blockchain ORDER BY insert_date DESC)) AS address,
  last_value(t.value :creator :: STRING) OVER (PARTITION BY t.value :address, blockchain ORDER BY insert_date DESC) AS creator,
  last_value(t.value :l1_label :: STRING) OVER (PARTITION BY t.value :address, blockchain ORDER BY insert_date DESC) AS l1_label,
  last_value(t.value :l2_label :: STRING) OVER (PARTITION BY t.value :address, blockchain ORDER BY insert_date DESC) AS l2_label,
  last_value(t.value :address_name :: STRING) OVER (PARTITION BY t.value :address, blockchain ORDER BY insert_date DESC) AS address_name, 
  last_value(t.value :project_name :: STRING) OVER (PARTITION BY t.value :address, blockchain ORDER BY insert_date DESC) AS project_name
FROM
  base_tables,
  LATERAL FLATTEN(
    input => record_content 
  ) t
