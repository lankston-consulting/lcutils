from src.lcutils import gcs

if __name__ == '__main__':
    gct = gcs.GcsTools()
    print(gct.list_blobs_names('fuelcast'))