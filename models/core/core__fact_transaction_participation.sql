{{ config(
  materialized = 'incremental',
  unique_key = 'fact_transaction_participation_id',
  incremental_strategy = 'merge',
  cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS(

  SELECT
    ROUND AS block_id,
    intra,
    algorand_decode_hex_addr(
      addr :: text
    ) AS address,
    DATEADD(
      ms,
      __HEVO__LOADED_AT,
      '1970-01-01'
    ) AS _INSERTED_TIMESTAMP
  FROM
    {{ source(
      'algorand',
      'TXN_PARTICIPATION'
    ) }}

{% if is_incremental() %}
WHERE
  _INSERTED_TIMESTAMP >= (
    SELECT
      MAX(
        _INSERTED_TIMESTAMP
      )
    FROM
      {{ this }}
  ) - INTERVAL '4 HOURS'
{% endif %}
)
SELECT
  {{ dbt_utils.surrogate_key(
    ['a.block_id','a.intra','a.address']
  ) }} AS fact_transaction_participation_id,
  ab.block_timestamp AS block_timestamp,
  A.block_id,
  COALESCE(
    ab.dim_block_id,
    {{ dbt_utils.surrogate_key(
      ['null']
    ) }}
  ) AS dim_block_id,
  A.intra,
  COALESCE(
    ad.dim_account_id,
    {{ dbt_utils.surrogate_key(
      ['null']
    ) }}
  ) AS dim_account_id,
  A.address,
  A._INSERTED_TIMESTAMP,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
  base A
  LEFT JOIN {{ ref('core__dim_block') }}
  ab
  ON A.block_id = ab.block_id
  LEFT JOIN {{ ref('core__dim_account') }}
  ad
  ON A.address = ad.address
