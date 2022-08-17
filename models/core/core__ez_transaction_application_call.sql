{{ config(
    materialized = 'view'
) }}

SELECT
    block_timestamp,
    block_timestamp :: DATE block_date,
    intra,
    tx_group_id,
    tx_id,
    inner_tx,
    tx_sender,
    fee,
    app_id,
    'appl' AS tx_type,
    'application call' AS tx_type_name,
    tx_message,
    extra
FROM
    {{ ref('core__fact_transaction') }}
WHERE
    dim_transaction_type_id = '63469c3c4f19f07c737127a117296de4'
