import requests

STAC_SMOS_ITEMS_URL = "https://datahub.creodias.eu/stac/collections/SMOS/items"

start = "2023-05-01"
stop = "2023-05-02"
bbox = 0, 40, 20, 60
limit = 120


params = [
    ("datetime", f"{start}T00:00:00.000Z/{stop}T23:59:59.999Z"),
    ("bbox", f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"),
    ("limit", f"{limit}"),
]

for i in range(0, 100):
    response = requests.get(STAC_SMOS_ITEMS_URL, params)
    response_obj = response.json()
    features = response_obj["features"]

    # del response_obj["features"]
    # print(json.dumps(response_obj, indent=2))

    sm_features = []
    os_features = []
    for feature in features:
        product_id = feature["id"]
        if "_MIR_SMUDP2_" in product_id:
            sm_features.append(feature)
        if "_MIR_OSUDP2_" in product_id:
            os_features.append(feature)

    for f in sm_features:
        print(f"{f['id']}", f"{f['properties']['productType']}")

    print(
        i,
        len(features),
        len(sm_features),
        len(os_features),
    )
