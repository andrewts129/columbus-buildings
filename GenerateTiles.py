from shapely import geometry, ops, wkt
import ijson.backends.yajl2_cffi as ijson
import csv
import geojson
import json
import geojson
import fiona
import pyproj
import subprocess
import sys
from functools import partial
from shapely.strtree import STRtree
from shapely.prepared import prep

GEOJSON_OUT = "viz/data.geojson"
MBTILES_OUT = "viz/data.mbtiles"

BUILDING_AGES_FILE = "data/ColumbusBuildingAges.csv"
PARCEL_AGES_FILE = "data/ParcelAges.csv"
PARCEL_SHAPES_FILE = "data/20181101_Parcel_Polygons/TAXPARCEL_CONDOUNITSTACK_LGIM.shp"
BUILDING_SHAPES_FILE = "data/BuildingFootprints/BUILDINGFOOTPRINT.shp"

IN_PROJ = pyproj.Proj(
    "+proj=lcc +lat_0=38 +lat_1=38.73333333 +lat_2=40.03333 +lon_0=-82.5 +x_0=600000 +y_0=0 +datum=NAD83 +units=us-ft +no_defs", preserve_units=True)
OUT_PROJ = pyproj.Proj(init="epsg:4326")

PROJECT = partial(pyproj.transform, IN_PROJ, OUT_PROJ)


def _contains_letters(s):
    return any(c.isalpha() for c in s)


def _sane_year_built(year):
    return 1776 <= int(year) <= 2027


def _clean_parcel_id(s):
    if s[-3:] == "-00":
        return s[:-3].strip().replace("-", "")
    else:
        return s.strip().replace("-", "")


def clean_coordinates(coordinates):
    if isinstance(coordinates, tuple):
        # if len(coordinates) < 3:
        #     converted_coords = pyproj.transform(IN_PROJ, OUT_PROJ, coordinates[0], coordinates[1])
        # else:
        #     converted_coords = pyproj.transform(IN_PROJ, OUT_PROJ, coordinates[0], coordinates[1], coordinates[2])
        return coordinates[0], coordinates[1]
    else:
        return [clean_coordinates(sublist) for sublist in coordinates]


def load_parcels():
    parcel_data = {}

    with fiona.open(PARCEL_SHAPES_FILE) as features:
        for feature in features:
            parcel_id = feature["properties"]["PARCELID"]
            parcel_year_built = feature["properties"]["RESYRBLT"]

            if not _contains_letters(parcel_id) and _sane_year_built(parcel_year_built) and feature["geometry"] is not None:
                parcel_address = feature["properties"]["SITEADDRES"]

                feature["geometry"]["coordinates"] = clean_coordinates(feature["geometry"]["coordinates"])
                parcel_shape = geometry.shape(feature["geometry"])
                parcel_shape = ops.transform(PROJECT, parcel_shape)

                parcel_data[parcel_shape.wkt] = {"id": parcel_id, "address": parcel_address,
                                                   "year_built": parcel_year_built}

                if len(parcel_data) % 10000 == 0:
                    print("Loaded " + str(len(parcel_data)) + " parcels...")

    return parcel_data


def load_buildings():
    building_shapes = []

    with fiona.open(BUILDING_SHAPES_FILE) as features:
        for feature in features:
            feature["geometry"]["coordinates"] = clean_coordinates(feature["geometry"]["coordinates"])
            building_shape = geometry.shape(feature["geometry"])
            building_shape = ops.transform(PROJECT, building_shape)
            building_shapes.append(building_shape)

            if len(building_shapes) % 10000 == 0:
                print("Loaded " + str(len(building_shapes)) + " building shapes...")

    return building_shapes


def match_buildings_to_parcels(parcel_data, building_shapes):
    # TODO remove this once I can actually get the code below to terminate before the heat death of the universe
    # return geojson.FeatureCollection(
    #     [geojson.Feature(geometry=f["geometry"], properties={"id": f["id"],
    #                                                          "address": f["address"],
    #                                                          "year_built": f["year_built"]}) for f in parcel_features])

    good_matches = 0
    partial_matches = 0
    non_matches = 0

    parcel_shapes = [wkt.loads(wkt_string) for wkt_string in parcel_data.keys()]
    parcel_shape_tree = STRtree(parcel_shapes)

    building_features = []

    for building_shape in building_shapes:
        is_partial = False
        matching_parcel_shape = None
        close_parcel_shapes = parcel_shape_tree.query(building_shape.buffer(0.001))

        for parcel_shape in close_parcel_shapes:
            if parcel_shape.contains(building_shape):
                matching_parcel_shape = parcel_shape
                good_matches += 1
                is_partial = False
                break
            elif parcel_shape.intersects(building_shape):
                matching_parcel_shape = parcel_shape
                is_partial = True

        if matching_parcel_shape is None:
            non_matches += 1
            building_feature = geojson.Feature(geometry=building_shape, properties={
                "id": "null",
                "address": "null",
                "year_built": 0
            })
        else:
            if is_partial:
                partial_matches += 1
            matching_parcel_feature = parcel_data[matching_parcel_shape.wkt]
            building_feature = geojson.Feature(geometry=building_shape, properties={
                "id": matching_parcel_feature["id"],
                "address": matching_parcel_feature["address"],
                "year_built": matching_parcel_feature["year_built"]
            })

        building_features.append(building_feature)

        if len(building_features) % 1000 == 0:
            print("Matched " + str(len(building_features)) + " buildings to parcel features! (G: " + str(good_matches) + ", P: " + str(partial_matches) + ", N: " + str(non_matches) + ")")

        if len(building_features) > 10000:
            break

    return geojson.FeatureCollection(building_features)


def main():
    parcel_data = load_parcels()
    print("Finished loading parcels!")

    building_shapes = load_buildings()
    print("Finished loading buildings!")

    building_features = match_buildings_to_parcels(parcel_data, building_shapes)
    print("Finished matching buildings to parcels!")

    with open(GEOJSON_OUT, "w") as file:
        geojson.dump(building_features, file)
    print("Finished dumping data to " + GEOJSON_OUT)

    tippecanoe_command = "tippecanoe -o " + MBTILES_OUT + " --maximum-zoom=16 --minimum-zoom=11 --force " + GEOJSON_OUT
    subprocess.call(tippecanoe_command.split(" "), stderr=sys.stderr, stdout=sys.stdout)
    print("Done!")


if __name__ == '__main__':
    main()
