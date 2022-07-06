from dotenv import load_dotenv

from src.lcutils import gcs
from src.lcutils import eet

import ee

load_dotenv()

if __name__ == "__main__":
    gct = gcs.GcsTools(use_service_account={'keyfile': 'fuelcast-storage-credentials.json'})
    # print(gct.list_blobs_names("fuelcast"))
    # eeh = eet.EeTools()

    # x = eeh.list_assets("projects/fuelcast/assSets", "emodis/emodis")

    # for d in x:
    #     print(d["id"])
    #     ee.data.deleteAsset(d['id'])

    # a = "fuelcast-zones-20k"

    # src = "projects/rd-general-projects/assets/rpms/" + a
    # dst = "projects/fuelcast/assets/rpms/" + a

    # ee.data.copyAsset(src, dst)

    surl = gct.generate_signed_url(
        "fuelcast-data", "projections/2022-06-10/annual_herb_ppa_2022-06-10.tif"
    )
    print(surl)
