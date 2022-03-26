{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', blockchain, address, creator)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'crosschain', 'address_labels', 'silver_crosschain__address_labels']
) }}

WITH base_tables AS (

  SELECT
    *,
    SPLIT(
      SUBSTR(
        record_metadata :key :: STRING,
        2,
        len(
          record_metadata :key :: STRING
        ) -2
      ),
      '-'
    ) [1] :: STRING AS blockchain,
    TO_TIMESTAMP(
      SPLIT(
        SUBSTR(
          record_metadata :key :: STRING,
          2,
          len(
            record_metadata :key :: STRING
          ) -2
        ),
        '-'
      ) [2] :: INT
    ) AS insert_date
  FROM
    {{ source(
      'bronze',
      'prod_address_label_sink_291098491'
    ) }}
  WHERE
    ARRAY_SIZE(
      SPLIT(
        SUBSTR(
          record_metadata :key :: STRING,
          2,
          len(
            record_metadata :key :: STRING
          ) -2
        ),
        '-'
      )
    ) = 3
    AND SPLIT(
      SUBSTR(
        record_metadata :key :: STRING,
        2,
        len(
          record_metadata :key :: STRING
        ) -2
      ),
      '-'
    ) [0] = 'labels'

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
), 

delete_table AS (
    SELECT 
        CASE WHEN blockchain IN (
            'algorand',
            'solana'
        ) THEN t.value :address :: STRING
        ELSE LOWER(
            t.value :address :: STRING
        ) END AS address,
        t.value :delete :: STRING AS delete_flag 
  
        FROM base_tables,
        LATERAL FLATTEN(
            input => record_content
        ) t
        
        WHERE delete_flag = 'true'
)

SELECT
  (
    record_metadata :CreateTime :: INT / 1000
  ) :: TIMESTAMP AS system_created_at,
  insert_date,
  blockchain,
  CASE
    WHEN blockchain IN (
      'algorand',
      'solana'
    ) THEN t.value :address :: STRING
    ELSE LOWER(
      t.value :address :: STRING
    )
  END AS address,
  t.value :creator :: STRING AS creator,
  t.value :l1_label :: STRING AS l1_label,
  t.value :l2_label :: STRING AS l2_label,
  t.value :address_name :: STRING AS address_name,
  t.value :project_name :: STRING AS project_name, 
    d.delete_flag
FROM
  base_tables b
  
  LEFT OUTER JOIN TABLE(FLATTEN(record_content)) t
  
  INNER JOIN delete_table d
  ON d.address = CASE
    WHEN blockchain IN (
      'algorand',
      'solana'
    ) THEN t.value :address :: STRING
    ELSE LOWER(
      t.value :address :: STRING
    )
  END 

  WHERE delete_flag IS NULL

qualify(ROW_NUMBER() over(PARTITION BY blockchain, address, creator
ORDER BY
  insert_date DESC)) = 1
