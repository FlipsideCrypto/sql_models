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
                table_name => '{{ source(
        'algorand_db_external',
        'algorand_indexer_tx'
    ) }}'
            )
        ) A
    GROUP BY
        last_modified,
        file_name
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

{% if is_incremental() %}
CROSS JOIN (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        ) max_INSERTED_TIMESTAMP
    FROM
        {{ this }}
) max_date
WHERE
    _PARTITION_BY_DATE >= max_INSERTED_TIMESTAMP :: DATE
    AND b.last_modified > max_INSERTED_TIMESTAMP
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY tx_id
ORDER BY
    _PARTITION_BY_DATE DESC)) = 1
