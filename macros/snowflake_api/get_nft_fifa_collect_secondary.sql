{% macro get_nft_fifa_collect_secondary() %}
  {% set query %}
  CREATE schema if NOT EXISTS bronze_api;
{% endset %}
  {% do run_query(query) %}
  {% set query %}
  CREATE TABLE if NOT EXISTS bronze_api.nft_fifa_collect_secondary(
    nft_asset_id INT,
    block_id INT,
    DATA STRING,
    _inserted_timestamp timestamp_ntz
  );
{% endset %}
  {% do run_query(query) %}
  {% set query %}
INSERT INTO
  bronze_api.nft_fifa_collect_secondary(
    nft_asset_id,
    block_id,
    DATA,
    _inserted_timestamp
  )
SELECT
  nft_asset_id,
  block_id,
  ethereum.streamline.udf_api(
    'GET',
    CONCAT(
      'https://api.prod.rock-palisade-352518.com/collectibles/activities?assetId=',
      nft_asset_id
    ),{},{}
  ) AS DATA,
  SYSDATE()
FROM
  (
    SELECT
      nft_asset_id,
      block_id
    FROM
      silver.nft_sales_fifa_collect
    WHERE
      TYPE = 'secondary'
    EXCEPT
    SELECT
      nft_asset_id,
      block_id
    FROM
      bronze_API.nft_fifa_collect_secondary
  )
LIMIT
  100;
{% endset %}
  {% do run_query(query) %}
{% endmacro %}
