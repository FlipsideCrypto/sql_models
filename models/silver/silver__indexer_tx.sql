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
                table_name => '{{ source( 'algorand_db_external', 'algorand_indexer_tx' ) }}'
            )
        ) A
    GROUP BY
        last_modified,
        file_name
)

{% if is_incremental() %},
max_date AS (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        ) max_INSERTED_TIMESTAMP
    FROM
        {{ this }}
)
{% endif %}
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
    tx_id IS NOT NULL
    AND _PARTITION_BY_DATE = '2022-07-27'

{% if is_incremental() %}
AND _PARTITION_BY_DATE >= (
    SELECT
        max_INSERTED_TIMESTAMP :: DATE
    FROM
        max_date
)
AND b.last_modified > (
    SELECT
        max_INSERTED_TIMESTAMP
    FROM
        max_date
)
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY tx_id
ORDER BY
    _PARTITION_BY_DATE DESC)) = 1
