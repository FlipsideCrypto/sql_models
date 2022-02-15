{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_id, tx_id)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['ingested_at::DATE'],
    tags = ['snowflake', 'solana', 'silver_solana', 'solana_sol_balances']
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    tx :meta :preBalances :: ARRAY AS sol_pre_balances,
    tx :meta :postBalances :: ARRAY AS sol_post_balances,
    ingested_at
FROM
    {{ ref('bronze_solana__transactions') }}
    t

{% if is_incremental() %}
WHERE
    ingested_at >= getdate() - INTERVAL '2 days'
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY block_id, tx_id
ORDER BY
    t.ingested_at DESC)) = 1
