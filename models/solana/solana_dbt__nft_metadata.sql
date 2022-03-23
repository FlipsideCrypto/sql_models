{{ config(
  materialized = 'incremental',
  unique_key = 'contract_address || token_id',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'crosschain', 'solana_dbt__nft_metadata']
) }}

WITH base_tables AS (

SELECT 
  *
FROM
  {{ source(
    'bronze',
    'prod_nft_metadata_uploads_1828572827'
  ) }}
WHERE
  SPLIT(
    record_content :model :sinks [0] :destination :: STRING,
    '.'
  ) [2] :: STRING = 'nft_metadata'
  AND record_content :model :blockchain :: STRING = 'solana'

{% if is_incremental() %}
AND (
  record_metadata :CreateTime :: INT / 1000
) :: TIMESTAMP :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(system_created_at :: DATE))
  FROM
    {{ this }}
)
{% endif %}

)

SELECT
  (
    record_metadata :CreateTime :: INT / 1000
  ) :: TIMESTAMP AS system_created_at,
  record_content:model:blockchain :: STRING AS blockchain,
  t.value :contract_name :: STRING AS contract_name,
  t.value :token_id :: STRING AS token_id, 
  t.value :mint_address :: STRING AS mint, 
  t.value :commission_rate :: FLOAT AS commission_rate,
  t.value :contract_address :: STRING AS contract_address,
  t.value :created_at_block_id :: bigint AS created_at_block_id,
  t.value :created_at_timestamp :: TIMESTAMP AS created_at_timestamp,
  t.value :created_at_tx_id :: STRING AS created_at_tx_id,
  t.value :creator_address :: STRING AS creator_address,
  t.value :creator_name :: STRING AS creator_name,
  t.value :image_url :: STRING AS image_url,
  t.value :project_name :: STRING AS project_name,
  t.value :token_metadata :: OBJECT AS token_metadata,
  t.value :token_metadata_uri :: STRING AS token_metadata_uri,
  t.value :token_name :: STRING AS token_name,*
FROM
  base_tables, 
  LATERAL FLATTEN(
    input => record_content: results
  ) t
