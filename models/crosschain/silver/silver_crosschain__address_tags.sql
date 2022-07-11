{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', blockchain, address, creator)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'crosschain', 'address_tags', 'tags']
) }}

SELECT 
    t.value :blockchain :: STRING AS blockchain, 
    t.value :source :: STRING AS creator, 
    t.value :address :: STRING AS address,
    t.value :tag_name :: STRING AS tag_name, 
    t.value :tag_type :: STRING AS tag_type, 
    t.value :start_date :: DATE AS start_date, 
    t.value :end_date :: DATE AS end_date, 
    TO_TIMESTAMP(record_metadata :CreateTime) AS _inserted_timestamp
FROM 
    {{ source(
      'bronze',
      'prod_address_tag_sync_1480319581'
    ) }}, 
       
LATERAL FLATTEN(
      input => record_content
    ) t

{% if is_incremental() %}
WHERE _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY blockchain, address, creator, tag_name
  ORDER BY
    _inserted_timestamp DESC)) = 1