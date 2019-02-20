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
        if len(coordinates) < 3:
            converted_coords = pyproj.transform(IN_PROJ, OUT_PROJ, coordinates[0], coordinates[1])
        else:
            converted_coords = pyproj.transform(IN_PROJ, OUT_PROJ, coordinates[0], coordinates[1], coordinates[2])
        return converted_coords[0], converted_coords[1]
    else:
        return [clean_coordinates(sublist) for sublist in coordinates]


def load_parcels():
    parcel_features = []

    with fiona.open(PARCEL_SHAPES_FILE) as features:
        for feature in features:
            parcel_id = feature["properties"]["PARCELID"]

            if not _contains_letters(parcel_id) and feature["geometry"] is not None:
                parcel_year_built = feature["properties"]["RESYRBLT"]
                parcel_address = feature["properties"]["SITEADDRES"]

                feature["geometry"]["coordinates"] = clean_coordinates(feature["geometry"]["coordinates"])
                parcel_shape = geometry.shape(feature["geometry"])

                new_feature = {"geometry": parcel_shape,
                               "id": parcel_id,
                               "address": parcel_address,
                               "year_built": parcel_year_built}

                parcel_features.append(new_feature)

                if len(parcel_features) % 10000 == 0:
                    print("Loaded " + str(len(parcel_features)) + " parcels...")

    return parcel_features


def load_buildings():
    building_shapes = []

    with fiona.open(BUILDING_SHAPES_FILE) as features:
        for feature in features:
            feature["geometry"]["coordinates"] = clean_coordinates(feature["geometry"]["coordinates"])
            building_shape = geometry.shape(feature["geometry"])
            building_shapes.append(building_shape)

            if len(building_shapes) % 10000 == 0:
                print("Loaded " + str(len(building_shapes)) + " building shapes...")

    return building_shapes


def match_buildings_to_parcels(parcel_features, building_shapes):
    # TODO remove this once I can actually get the code below to terminate before the heat death of the universe
    return geojson.FeatureCollection(parcel_features)

    good_matches = 0
    partial_matches = 0
    non_matches = 0

    building_features = []

    for building_shape in building_shapes:
        full_match = False
        matching_parcel = None

        for parcel_feature in parcel_features:
            if parcel_feature["geometry"].contains(building_shape):
                matching_parcel = parcel_feature
                full_match = True
                break
            elif parcel_feature["geometry"].intersects(building_shape):
                matching_parcel = parcel_feature

        if matching_parcel is not None:
            new_feature = geojson.Feature(geometry=building_shape,
                                          properties={"id": matching_parcel["id"],
                                                      "address": matching_parcel["address"],
                                                      "year_built": matching_parcel["year_built"]})
            building_features.append(new_feature)

            if full_match:
                good_matches += 1
            else:
                partial_matches += 1

            if len(building_features) % 100 == 0:
                print("Matched " + str(len(building_features)) + " buildings to features. (" + str(good_matches) + " full, " + str(partial_matches) + " partial, " + str(non_matches) + " failures)")
        else:
            non_matches += 1

            if non_matches % 10 == 0:
                print("(" + str(non_matches) + " failures in matching so far)")

    return geojson.FeatureCollection(building_features)


def main():
    parcel_features = load_parcels()
    print("Finished loading parcels!")

    building_shapes = load_buildings()
    print("Finished loading buildings!")

    building_features = match_buildings_to_parcels(parcel_features, building_shapes)
    print("Finished matching buildings to parcels!")

    with open(GEOJSON_OUT, "w") as file:
        geojson.dump(building_features, file)
    print("Finished dumping data to " + GEOJSON_OUT)

    tippecanoe_command = "tippecanoe -o " + MBTILES_OUT + " -zg --drop-densest-as-needed --force " + GEOJSON_OUT
    subprocess.call(tippecanoe_command.split(" "), stderr=sys.stderr, stdout=sys.stdout)
    print("Done!")


if __name__ == '__main__':
    main()
