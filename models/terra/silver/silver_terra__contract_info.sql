{{ config(
  materialized = 'incremental',
  unique_key = "_UK",
  incremental_strategy = 'merge',
  tags = ['snowflake', 'terra', 'decoded', 'terra_contracts']
) }}

SELECT
  concat_ws(
    '-',
    DATA :name :: STRING,
    DATA :request_meta.path :: STRING,
    flt.value :contract_info.address :: STRING
  ) AS _UK,
  DATA :name :: STRING AS source,
  DATA :request_meta.path :: STRING AS route,
  flt.value :contract_info.address :: STRING AS address,
  flt.value :contract_info.admin :: STRING AS admin,
  flt.value :contract_info.code_id :: INTEGER AS code_id,
  flt.value :contract_info.creator :: STRING AS creator,
  flt.value :contract_info.init_msg :: variant AS init_msg,
  SYSDATE() AS _inserted_timestamp
FROM
  {{ source(
    'bronze',
    'prod_terra_api'
  ) }}
  src,
  LATERAL FLATTEN (
    DATA :response_data
  ) flt
WHERE
  DATA :request_meta.path = '/bulk_get_contract_info'

{% if is_incremental() %}
AND src._inserted_timestamp >= COALESCE(
  (
    SELECT
      MAX(_inserted_timestamp)
    FROM
      {{ this }}
  ),
  '1900-01-01'
)
{% endif %}

qualify ROW_NUMBER() over (
  PARTITION BY _UK
  ORDER BY
    _inserted_timestamp
) = 1
