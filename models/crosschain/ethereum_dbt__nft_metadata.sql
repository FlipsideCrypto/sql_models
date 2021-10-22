{{ config(
  materialized = 'incremental',
  unique_key = 'blockchain || address || creator',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'crosschain', 'ethereum_dbt__nft_metadata']
) }}

WITH base_tables AS (

  SELECT
    *
  FROM
    {{ source(
      'bronze',
      'PROD_ETHEREUM_SINK_407559501'
    ) }}
  WHERE
    SPLIT(
      record_content :model :sinks [0] :destination :: STRING,
      '.'
    ) [2] :: STRING = 'nft_metadata'

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
  'Ethereum' AS blockchain,
  VALUE :commission_rate :: FLOAT AS commission_rate,
  VALUE :contract_address :: STRING AS contract_address,
  VALUE :contract_name :: STRING AS contract_name,
  VALUE :created_at_block_id :: TIMESTAMP AS created_at_block_id,
  VALUE :created_at :: TIMESTAMP AS created_at_timestamp,
  VALUE :created_at_tx_id :: INTEGER AS created_at_tx_id,
  VALUE :creator_address :: STRING AS creator_address,
  VALUE :creator_name :: STRING AS creator_name,
  VALUE :src :: STRING AS image_url,
  VALUE :project_name :: STRING AS project_name,
  VALUE :token_id :: STRING AS token_id,
  VALUE :token_metadata AS token_metadata,
  VALUE :token_metadata_uri :: STRING AS token_metadata_uri,
  VALUE :name AS token_name
FROM
  base_tables,
  LATERAL FLATTEN(
    input => record_content
  ) t
