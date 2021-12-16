{{ config(
  materialized = 'incremental',
  unique_key = "block_id",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'silver_solana', 'solana_transactions']
) }}

