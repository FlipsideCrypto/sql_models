{{ config(
    materialized = 'view'
) }}

SELECT
    b.block_timestamp,
    block_date,
    b.block_id,
    intra,
    tx_group_id,
    tx_id,
    inner_tx,
    b.tx_sender,
    fee,
    ast.asset_id,
    ast.asset_name,
    asset_supply,
    asset_parameters,
    'acfg' AS tx_type,
    'asset configuration' AS tx_type_name,
    tx_message,
    extra,
    b._inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    {{ ref('core__fact_transaction') }}
    b
    JOIN {{ ref('core__dim_block') }} C
    ON b.dim_block_id = C.dim_block_id
    JOIN {{ ref('core__dim_asset') }}
    ast
    ON b.dim_asset_id = ast.dim_asset_id
WHERE
    b.dim_transaction_type_id = '09b31a7810640ff01202b26dd70a7aa3'
