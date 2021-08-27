{{ config(
  materialized = 'incremental',
  unique_key = 'blockchain || address',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_id, 'block_timestamp'],
  tags = ['snowflake', 'terra_gold', 'terra_address_labels']
) }}

SELECT
  blockchain,
  address,
  tag_name,
  tag_type,
  tag_slug,
  source,
  start_date,
  end_date
FROM
  {{ source(
    'shared',
    'udm_address_tags'
  ) }}
WHERE
  blockchain = 'terra'
