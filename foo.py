# %%
# %%
import pandas as pd
import json
import requests

# %%
df = pd.read_json(
    "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?CMC_PRO_API_KEY=3ab21cb9-7870-40b3-8291-de0794549736",
    lines=True,
)

# %%
data = json.loads(
    # "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?CMC_PRO_API_KEY=3ab21cb9-7870-40b3-8291-de0794549736",
)
# %%
prices = pd.DataFrame(df["data"][0],)
# %%
meta = pd.DataFrame(
    pd.read_json("https://api.coingecko.com/api/v3/coins/list?include_platform=true"),
)
# meta = pd.DataFrame(
#     pd.read_json(
#         "https://pro-api.coinmarketcap.com/v1/cryptocurrency/map?CMC_PRO_API_KEY=3ab21cb9-7870-40b3-8291-de0794549736&sort=cmc_rank&aux=platform,first_historical_data,last_historical_data,is_active,status&listing_status=active,inactive,untracked", lines=True
#     )["data"][0],
# )

# %%


def get_cmc_metadata():
    page = 1
    limit = 5000
    loop = True
    provider = "coinmarketcap"
    start = 1
    asset_data = []

    while loop:
        CMC_LISTING_URL = f"https://pro-api.coinmarketcap.com/v1/cryptocurrency/map?CMC_PRO_API_KEY=3ab21cb9-7870-40b3-8291-de0794549736&limit=5000&sort=cmc_rank&aux=platform&start={start}"
        raw = requests.get(CMC_LISTING_URL)
        response = raw.json()
        data = response["data"]

        for asset in data:
            platform_data = asset.get("platform")
            platform_id, token_address, platform = "layer-1", None, None
            asset_id = asset.get("id")
            symbol = asset.get("symbol")
            name = asset.get("name")
            slug = asset.get("slug")

            if platform_data:
                platform_id = platform_data.get("id")
                platform = platform_data.get("slug")
                token_address = platform_data.get("token_address")

            asset_data.append(
                {
                    "provider": provider,
                    "asset_id": asset_id,
                    "symbol": symbol,
                    "name": name,
                    "slug": slug,
                    "platform_id": platform_id,
                    "platform": platform,
                    "token_address": token_address,
                }
            )

        if len(data) < 5000:
            loop = False
        else:
            start = start + 5000
    return asset_data


# %%
raw = get_cmc_metadata()
# %%
dp = pd.DataFrame(raw)
# %%
