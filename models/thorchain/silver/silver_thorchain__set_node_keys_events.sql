{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'set_node_keys_events']
) }}

SELECT
  *
FROM
  {{ ref('thorchain_dbt__set_node_keys_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY node_addr, secp256k1, ed25519, block_timestamp, validator_consensus
ORDER BY
  __HEVO__INGESTED_AT DESC)) = 1

{% if is_incremental() %}
WHERE
  __HEVO_loaded_at >= (
    SELECT
      MAX(__HEVO_loaded_at)
    FROM
      {{ this }}
  )
{% endif %}
