{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = 'block_id',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp', 'contract_address'],
  tags = ['snowflake', 'ethereum', 'events_emitted', 'address_labels']
) }}

WITH eth_labels AS (

  SELECT
    l1_label,
    l2_label,
    project_name,
    address_name,
    address
  FROM
    {{ ref('silver_crosschain__address_labels') }}
  WHERE
    blockchain = 'ethereum'
    AND creator = 'flipside'
)
SELECT
  DISTINCT block_id AS block_id,
  block_timestamp AS block_timestamp,
  tx_id AS tx_id,
  event_index AS event_index,
  event_inputs AS event_inputs,
  event_name AS event_name,
  event_removed AS event_removed,
  tx_from_addr AS tx_from_address,
  COALESCE(
    from_labels.l1_label,
    'unlabeled'
  ) AS tx_from_label_type,
  COALESCE(
    from_labels.l2_label,
    'unlabled'
  ) AS tx_from_label_subtype,
  COALESCE(
    from_labels.project_name,
    'unlabeled'
  ) AS tx_from_label,
  COALESCE(
    from_labels.address_name,
    'unlabeled'
  ) AS tx_from_address_name,
  tx_to_addr AS tx_to_address,
  COALESCE(
    to_labels.l1_label,
    'unlabeled'
  ) AS tx_to_label_type,
  COALESCE(
    to_labels.l2_label,
    'unlabeled'
  ) AS tx_to_label_subtype,
  COALESCE(
    to_labels.project_name,
    'unlabeled'
  ) AS tx_to_label,
  COALESCE(
    to_labels.address_name,
    'unlabeled'
  ) AS tx_to_address_name,
  contract_addr AS contract_address,
  COALESCE(
    contract_labels.address_name,
    contract_name,
    'unlabeled'
  ) AS contract_name,
  tx_succeeded AS tx_succeeded
FROM
  {{ ref('silver_ethereum__events_emitted') }}
  b
  LEFT OUTER JOIN eth_labels AS from_labels
  ON b.tx_from_addr = from_labels.address
  LEFT OUTER JOIN eth_labels AS to_labels
  ON b.tx_to_addr = to_labels.address
  LEFT OUTER JOIN eth_labels AS contract_labels
  ON b.contract_addr = contract_labels.address
WHERE
  1 = 1

{% if is_incremental() %}
AND b.block_timestamp >= getdate() - INTERVAL '40 hours'
{% endif %}
