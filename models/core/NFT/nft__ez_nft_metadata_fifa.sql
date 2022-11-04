{{ config(
    materialized = 'view'
) }}

SELECT
    nft_asset_id,
    nft_asset_name,
    nft_url,
    metadata_url,
    animation_url,
    animation_url_mimetype,
    description,
    external_url,
    external_url_mimetype,
    image,
    image_integrity,
    image_mimetype,
    NAME,
    rekey_checked,
    application_id,
    drop_number,
    drop_name,
    item_no,
    editions,
    rarity,
    world_cup_type,
    YEAR,
    host,
    country_1,
    country_1_score,
    country_2,
    country_2_score,
    event_type,
    player,
    MINUTE
FROM
    {{ ref('silver__nft_metadata_fifa') }}
