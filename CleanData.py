from shapely import geometry
import ijson.backends.yajl2_cffi as ijson
import csv
import geojson
import json
import geojson
import fiona
import pyproj
import subprocess
import sys

GEOJSON_OUT = "viz/data.geojson"
MBTILES_OUT = "viz/data.mbtiles"

BUILDING_AGES_FILE = "data/ColumbusBuildingAges.csv"
PARCEL_AGES_FILE = "data/ParcelAges.csv"
PARCEL_SHAPES_FILE = "data/20181101_Parcel_Polygons/TAXPARCEL_CONDOUNITSTACK_LGIM.shp"
BUILDING_SHAPES_FILE = "data/BuildingFootprints/BUILDINGFOOTPRINT.shp"

IN_PROJ = pyproj.Proj("+proj=lcc +lat_0=38 +lat_1=38.73333333 +lat_2=40.03333 +lon_0=-82.5 +x_0=600000 +y_0=0 +datum=NAD83 +units=us-ft +no_defs")
OUT_PROJ = pyproj.Proj(init="epsg:4326")


def _contains_letters(s):
    return any(c.isalpha() for c in s)


def _clean_parcel_id(s):
    if s[-3:] == "-00":
        return s[:-3].strip().replace("-", "")
    else:
        return s.strip().replace("-", "")


def clean_coordinates(coordinates):
    if isinstance(coordinates, tuple):
        converted_coords = pyproj.transform(IN_PROJ, OUT_PROJ, coordinates[0], coordinates[1], coordinates[2])
        return converted_coords[0], converted_coords[1]
    else:
        return [clean_coordinates(x) for x in coordinates]


def load_parcels():
    parcel_features = []

    with fiona.open(PARCEL_SHAPES_FILE) as features:
        for feature in features:
            parcel_id = feature["properties"]["PARCELID"]

            if not _contains_letters(parcel_id) and feature["geometry"] is not None:
                parcel_year_built = feature["properties"]["RESYRBLT"]
                parcel_address = "TODO"
                parcel_shape = geometry.shape(feature["geometry"])

                new_feature = geojson.Feature(
                    geometry=parcel_shape,
                    properties={
                        "id": parcel_id,
                        "address": parcel_address,
                        "year_built": parcel_year_built
                    }
                )

                parcel_features.append(new_feature)

                if len(parcel_features) % 10000 == 0:
                    print("Loaded " + str(len(parcel_features)) + " parcels...")

    return geojson.FeatureCollection(parcel_features)


def load_buildings():
    building_shapes = []

    with fiona.open(BUILDING_SHAPES_FILE) as features:
        for feature in features:
            building_shape = geometry.shape(feature["geometry"])
            building_shapes.append(building_shape)

            if len(building_shapes) % 10000 == 0:
                print("Loaded " + str(len(building_shapes)) + " building shapes...")

    return building_shapes


def main():
    parcels = load_parcels()
    print("Finished loading parcels!")

    buildings = load_buildings()
    print("Finished loading buildings!")

    with open(GEOJSON_OUT, "w") as file:
        geojson.dump(parcels, file)

    tippecanoe_command = "tippecanoe -o " + MBTILES_OUT + " -zg --drop-densest-as-needed --force " + GEOJSON_OUT
    subprocess.call(tippecanoe_command.split(" "), stderr=sys.stderr, stdout=sys.stdout)
    print("Done!")


if __name__ == '__main__':
    main()
