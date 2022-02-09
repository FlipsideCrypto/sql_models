{{ config(
  materialized = 'table',
  unique_key = 'label',
  tags = ['snowflake', 'terra', 'labels', 'address_labels']
) }}

WITH base AS (

  SELECT
    project_name AS label,
    MAX(
      CASE
        WHEN address_name = 'operator_address' THEN address
        ELSE NULL
      END
    ) AS operator_address,
    MAX(
      CASE
        WHEN address_name = 'delegator_address' THEN address
        ELSE NULL
      END
    ) AS delegator_address,
    MAX(
      CASE
        WHEN address_name = 'vp_address' THEN address
        ELSE NULL
      END
    ) AS vp_address
  FROM
    {{ ref('silver_crosschain__address_labels') }}
  WHERE
    blockchain = 'terra'
    AND l1_label = 'operator'
  GROUP BY
    label
)
SELECT
  *
FROM
  base
WHERE
  operator_address IS NOT NULL
  OR delegator_address IS NOT NULL
  OR vp_address IS NOT NULL
