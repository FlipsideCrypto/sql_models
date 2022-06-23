{{ config(
    materialized = 'incremental',
    unique_key = 'TX_ID',
    incremental_strategy = 'merge',
    cluster_by = ['_INSERTED_TIMESTAMP::DATE']
) }}

WITH meta AS (

    SELECT
        last_modified,
        file_name
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '"FLIPSIDE_PROD_DB"."BRONZE"."ALGORAND_INDEXER_TX"'
            )
        ) A
    GROUP BY
        1,
        2
)
SELECT
    tx_id,
    account_id,
    DATA :"confirmed-round" :: INT AS block_id,
    DATA,
    last_modified AS _INSERTED_TIMESTAMP
   
FROM
    {{ source(
        'algorand_db_external',
        'algorand_indexer_tx'
    ) }} 
    JOIN meta b
    ON b.file_name = metadata$filename
WHERE
    1 = 1

{% if is_incremental() %}
AND _PARTITION_BY_DATE > CURRENT_DATE - 2
AND b.last_modified > (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY tx_id
ORDER BY
    _PARTITION_BY_DATE DESC)) = 1
