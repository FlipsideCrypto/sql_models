{{ config(
    materialized = 'view'
) }}

SELECT
    b.block_timestamp,
    block_date,
    block_id,
    intra,
    tx_group_id,
    tx_id,
    inner_tx,
    b.tx_sender,
    fee,
    app_id,
    'appl' AS tx_type,
    'application call' AS tx_type_name,
    tx_message,
    extra,
    b._inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    {{ ref('core__fact_transaction') }}
    b
    JOIN {{ ref('core__dim_block') }} C
    ON b.dim_block_id = C.dim_block_id
WHERE
    b.dim_transaction_type_id = '63469c3c4f19f07c737127a117296de4'
