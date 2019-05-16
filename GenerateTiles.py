from shapely import geometry, ops, wkt
import json
import geojson
import fiona
import pyproj
import subprocess
import sys
from functools import partial
from shapely.strtree import STRtree
from shapely.prepared import prep
import multiprocessing as mp
from itertools import repeat
from datetime import datetime
import logging
import math
import requests
import re
import os


GEOJSON_OUT = "viz/data.geojson"
MBTILES_OUT = "viz/data.mbtiles"

BUILDING_AGES_FILE = "data/ColumbusBuildingAges.csv"
PARCEL_AGES_FILE = "data/ParcelAges.csv"
PARCEL_SHAPES_FILE = "data/20181101_Parcel_Polygons/TAXPARCEL_CONDOUNITSTACK_LGIM.shp"
BUILDING_SHAPES_FILE = "data/BuildingFootprints/BUILDINGFOOTPRINT.shp"

OSU_BUILDING_DATA_FILE = "data/OhioState/data.gdb"
OSU_BUILDING_AGES_FILE = "data/OhioState/ages.json"
OSU_BUILDING_DETAILS_ENDPOINT = "https://gismaps.osu.edu/OSUDataService/OSUService.svc/BuildingDetailsExtended"

IN_PROJ_FRANKLIN = pyproj.Proj(
    "+proj=lcc +lat_0=38 +lat_1=38.73333333 +lat_2=40.03333 +lon_0=-82.5 +x_0=600000 +y_0=0 +datum=NAD83 +units=us-ft +no_defs",
    preserve_units=True)
IN_PROJ_OSU = pyproj.Proj(
    "+proj=lcc +lat_0=38 +lat_1=38.373333333 +lat_2=40.03333 +lon_0=-82.5 +x_0=50000 +y_0=0 +datum=WGS84 +units=us-in +no_defs",
    preserve_units=True)
OUT_PROJ = pyproj.Proj(init="epsg:4326")

PROJECT_FRANKLIN = partial(pyproj.transform, IN_PROJ_FRANKLIN, OUT_PROJ)
PROJECT_OSU = partial(pyproj.transform, IN_PROJ_OSU, OUT_PROJ)


logging.basicConfig(filename="loggy.log", format='%(asctime)s %(message)s', level=logging.INFO)


class MatchStatus:
    NONE = 1
    PARTIAL = 2
    FULL = 3


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
        #     converted_coords = pyproj.transform(IN_PROJ_FRANKLIN, OUT_PROJ, coordinates[0], coordinates[1])
        # else:
        #     converted_coords = pyproj.transform(IN_PROJ_FRANKLIN, OUT_PROJ, coordinates[0], coordinates[1], coordinates[2])
        return coordinates[0], coordinates[1]
    else:
        return [clean_coordinates(sublist) for sublist in coordinates]


def parse_parcel_feature(parcel_feature):
    parcel_id = parcel_feature["properties"]["PARCELID"]

    parcel_year_built = parcel_feature["properties"]["RESYRBLT"]

    if not _contains_letters(parcel_id) and _sane_year_built(parcel_year_built) and parcel_feature["geometry"] is not None:
        parcel_address = parcel_feature["properties"]["SITEADDRES"]

        parcel_feature["geometry"]["coordinates"] = clean_coordinates(parcel_feature["geometry"]["coordinates"])
        parcel_shape = geometry.shape(parcel_feature["geometry"])
        parcel_shape = ops.transform(PROJECT_FRANKLIN, parcel_shape)

        return (parcel_shape.wkt, {"id": parcel_id, "address": parcel_address,
                                   "year_built": parcel_year_built})
    else:
        return None


def load_parcels():
    parcel_data = {}

    pool = mp.Pool()

    with fiona.open(PARCEL_SHAPES_FILE) as features:
        for result in pool.imap_unordered(parse_parcel_feature, features, chunksize=1000):
            if result is not None:
                parcel_data[result[0]] = result[1]
                if len(parcel_data) % 10000 == 0:
                    print("Loaded " + str(len(parcel_data)) + " parcels...")

    pool.terminate()
    pool.join()

    return parcel_data


def parse_building_feature(building_feature):
    building_feature["geometry"]["coordinates"] = clean_coordinates(building_feature["geometry"]["coordinates"])
    building_shape = geometry.shape(building_feature["geometry"])
    building_shape = ops.transform(PROJECT_FRANKLIN, building_shape)
    return building_shape


def load_buildings():
    building_shapes = []

    pool = mp.Pool()

    with fiona.open(BUILDING_SHAPES_FILE) as features:
        for result in pool.imap_unordered(parse_building_feature, features, chunksize=1000):
            building_shapes.append(result)

            if len(building_shapes) % 10000 == 0:
                print("Loaded " + str(len(building_shapes)) + " building shapes...")

    pool.terminate()
    pool.join()

    return building_shapes


def find_matching_parcel_wrapper(args):
    def find_matching_parcel(building_shape, parcel_shape_tree):
        match_status = MatchStatus.PARTIAL

        matching_parcel_shape = None
        close_parcel_shapes = parcel_shape_tree.query(building_shape.buffer(0.001))

        prep_building_shape = prep(building_shape)
        for parcel_shape in close_parcel_shapes:
            if prep_building_shape.within(parcel_shape):
                matching_parcel_shape = parcel_shape
                match_status = MatchStatus.FULL
                break
            elif prep_building_shape.intersects(parcel_shape):
                matching_parcel_shape = parcel_shape

        if matching_parcel_shape is None:
            return MatchStatus.NONE, building_shape, None
        else:
            return match_status, building_shape, matching_parcel_shape.wkt

    logging.info(mp.current_process().name + ": " + args[0].wkt)
    try:
        return find_matching_parcel(args[0], args[1])
    except Exception as e:
        logging.exception(e)
        return MatchStatus.NONE, args[0], None


def match_buildings_to_parcels(parcel_data, building_shapes):
    non_matches = 0
    partial_matches = 0
    full_matches = 0

    parcel_shapes = [wkt.loads(wkt_string) for wkt_string in parcel_data.keys()]
    parcel_shape_tree = STRtree(parcel_shapes)

    building_features = []

    pool = mp.Pool()

    for result in pool.imap_unordered(find_matching_parcel_wrapper, zip(building_shapes, repeat(parcel_shape_tree)), chunksize=math.ceil(len(building_shapes) / 8)):
        match_status, building_shape, matching_parcel_shape_wkt = result

        if match_status == MatchStatus.NONE:
            non_matches += 1

            building_feature = geojson.Feature(geometry=building_shape, properties={
                "id": "null",
                "address": "null",
                "year_built": 0
            })
        else:
            if match_status == MatchStatus.PARTIAL:
                partial_matches += 1
            elif match_status == MatchStatus.FULL:
                full_matches += 1

            matching_parcel_feature = parcel_data[matching_parcel_shape_wkt]
            building_feature = geojson.Feature(geometry=building_shape, properties={
                "id": matching_parcel_feature["id"],
                "address": matching_parcel_feature["address"],
                "year_built": matching_parcel_feature["year_built"]
            })

        building_features.append(building_feature)

        if len(building_features) % 500 == 0:
            print("Matched " + str(len(building_features)) + " buildings to parcel features! (F: " + str(full_matches) + ", P: " + str(partial_matches) + ", N: " + str(non_matches) + ")")

    pool.terminate()
    pool.join()

    return building_features


def load_osu_building_ages():
    def get_building_age(building_number):
        response = requests.get(f"{OSU_BUILDING_DETAILS_ENDPOINT}/{building_number}")
        build_year_group = re.search('"Date Constructed":"(\\d*)/', response.text)
        if build_year_group is None:
            return "0"
        else:
            return build_year_group.group(1)

    if os.path.isfile(OSU_BUILDING_AGES_FILE):
        with open(OSU_BUILDING_AGES_FILE, "r") as f:
            building_ages = json.load(f)
            print(f"Loaded {len(building_ages)} OSU building ages from {OSU_BUILDING_AGES_FILE}...")
            return building_ages
    else:
        building_ages = {}
        with fiona.open(OSU_BUILDING_DATA_FILE) as features:
            building_numbers = [str(feature["properties"]["BldgNumber"]) for feature in features]

            # Not parallelized to avoid rate-limiting
            for num in building_numbers:
                if num != "None" and num != "0":
                    building_ages[num] = get_building_age(num)
                    print(f"Loaded {len(building_ages)} OSU building ages from {OSU_BUILDING_DETAILS_ENDPOINT}...")

        print(f"Writing {len(building_ages)} OSU building ages to {OSU_BUILDING_AGES_FILE}...")
        with open(OSU_BUILDING_AGES_FILE, "w") as f:
            json.dump(building_ages, f)

        return building_ages


def parse_osu_building_feature_wrapper(args):
    def parse_osu_building_feature(feature, building_ages):
        building_shape = geometry.shape(feature["geometry"])
        building_shape = ops.transform(PROJECT_OSU, building_shape)
        building_address = feature["properties"]["Address"]

        building_year = building_ages.get(str(feature["properties"]["BldgNumber"]))
        if building_year is None:
            building_year = "0"

        return geojson.Feature(geometry=building_shape, properties={
            "address": building_address,
            "year_built": building_year
        })

    return parse_osu_building_feature(args[0], args[1])


def load_osu_building_features():
    building_ages = load_osu_building_ages()
    building_features = []
    pool = mp.Pool()

    with fiona.open(OSU_BUILDING_DATA_FILE) as features:
        for result in pool.imap_unordered(parse_osu_building_feature_wrapper, zip(features, repeat(building_ages)), chunksize=200):
            building_features.append(result)
            if len(building_features) % 100 == 0:
                print(f"Loaded {len(building_features)} OSU building features...")

    pool.terminate()
    pool.join()

    return building_features


def main():
    start_time = datetime.now()

    osu_building_features = load_osu_building_features()

    franklin_parcel_data = load_parcels()
    franklin_building_shapes = load_buildings()
    franklin_building_features = match_buildings_to_parcels(franklin_parcel_data, franklin_building_shapes)

    all_building_features = geojson.FeatureCollection(osu_building_features + franklin_building_features)

    with open(GEOJSON_OUT, "w") as file:
        geojson.dump(all_building_features, file)
    print("Finished dumping data to " + GEOJSON_OUT)

    tippecanoe_command = "tippecanoe -Z12 -z15 -o " + MBTILES_OUT + " --coalesce-smallest-as-needed --extend-zooms-if-still-dropping --include=year_built --force " + GEOJSON_OUT
    subprocess.call(tippecanoe_command.split(" "), stderr=sys.stderr, stdout=sys.stdout)
    print("Done! (Total time: " + str(datetime.now() - start_time) + ")")


if __name__ == '__main__':
    main()
