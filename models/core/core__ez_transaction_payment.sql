{{ config(
    materialized = 'view'
) }}

SELECT
    b.block_timestamp,
    block_timestamp :: DATE block_date,
    block_id,
    intra,
    tx_group_id,
    tx_id,
    inner_tx,
    b.tx_sender,
    fee,
    ast.asset_id,
    ast.asset_name,
    receiver,
    amount / pow(
        10,
        6
    ) AS amount,
    'pay' AS tx_type,
    'payment' AS tx_type_name,
    tx_message,
    extra
FROM
    {{ ref('core__fact_transaction') }}
    b
    JOIN {{ ref('core__dim_asset') }}
    ast
    ON b.dim_asset_id = ast.dim_asset_id
WHERE
    b.dim_transaction_type_id = 'b02a45a596bfb86fe2578bde75ff5444'
