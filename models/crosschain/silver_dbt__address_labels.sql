{{ config(
  materialized = 'incremental',
  unique_key = 'blockchain || address || creator',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'crosschain', 'address_labels', 'silver_crosschain__address_labels']
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
  LOWER(t.value :address :: STRING)AS address,
  t.value :creator :: STRING AS creator,
  t.value :l1_label :: STRING AS l1_label,
  t.value :l2_label :: STRING AS l2_label,
  t.value :address_name :: STRING AS address_name, 
  t.value :project_name :: STRING AS project_name
FROM
  base_tables,
  LATERAL FLATTEN(
    input => record_content 
  ) t
