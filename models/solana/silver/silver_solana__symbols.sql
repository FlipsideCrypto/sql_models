{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', token_address, symbol)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'solana', 'silver_solana', 'solana_symbols']
) }}

SELECT 
    m.asset_id, 
    p.name, 
    p.symbol, 
    m.token_address, 
    p.recorded_at
FROM {{ source('shared', 'prices_v2')}} p 
JOIN {{ source('shared', 'market_asset_metadata') }} m ON m.asset_id = p.asset_id

WHERE m.token_address IS NOT NULL
AND m.platform = 'solana'
AND recorded_at >= '2021-11-28 00:00:00.000'

{% if is_incremental() %}
    AND recorded_at >= (
      SELECT
        MAX(
          recorded_at
        )
      FROM
        {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY m.token_address, p.symbol
ORDER BY
  recorded_at DESC)) = 1