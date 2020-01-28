import itertools
import json
import logging
import math
import multiprocessing as mp
import os
import re
import subprocess
import sys
import tempfile
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
from enum import Enum
from ftplib import FTP
from functools import partial
from pathlib import Path
from typing import Tuple, Union, Dict, Optional, List, Iterable
from zipfile import ZipFile

import fiona
import geojson
import pyproj
import requests
from geojson import Feature
from shapely import geometry, ops, wkt
from shapely.geometry import Polygon
from shapely.prepared import prep
from shapely.strtree import STRtree

GEOJSON_OUT = "viz/buildings.geojson"
MBTILES_OUT = "viz/buildings.mbtiles"

# From https://gismaps.osu.edu/OSUMaps/Default.html?#
OSU_BUILDING_DATA_FILE = "data/OhioState/data.gdb"
OSU_BUILDING_DETAILS_ENDPOINT = "https://gismaps.osu.edu/OSUDataService/OSUService.svc/BuildingDetailsExtended"

# Generated by a previous run of this script
OSU_BUILDING_AGES_FILE = "data/OhioState/ages.json"

IN_PROJ_FRANKLIN = pyproj.Proj(
    "+proj=lcc +lat_0=38 +lat_1=38.73333333333333 +lat_2=40.03333333333333 +lon_0=-82.5 +x_0=600000 +y_0=0 +datum=NAD83 +units=us-ft +no_defs",
    preserve_units=True)
IN_PROJ_OSU = pyproj.Proj(
    "+proj=lcc +lat_0=38 +lat_1=38.37333333333334 +lat_2=40.03333333333333 +lon_0=-82.5 +x_0=50000 +y_0=0 +datum=WGS84 +units=us-in +no_defs",
    preserve_units=True)
OUT_PROJ = pyproj.Proj(init="epsg:4326")

PROJECT_FRANKLIN = partial(pyproj.transform, IN_PROJ_FRANKLIN, OUT_PROJ)
PROJECT_OSU = partial(pyproj.transform, IN_PROJ_OSU, OUT_PROJ)

logging.basicConfig(filename="loggy.log", format='%(asctime)s %(message)s', level=logging.INFO)


class MatchStatus(Enum):
    NONE = 1
    PARTIAL = 2
    FULL = 3


def _contains_letters(s: str) -> bool:
    return any(c.isalpha() for c in s)


def _sane_year_built(year: int) -> bool:
    return 1776 <= int(year) <= 2027


def _clean_parcel_id(s: str) -> str:
    if s[-3:] == "-00":
        return s[:-3].strip().replace("-", "")
    else:
        return s.strip().replace("-", "")


# TODO typing
def clean_coordinates(coordinates):
    if isinstance(coordinates, tuple):
        return coordinates[0], coordinates[1]
    else:
        return [clean_coordinates(sublist) for sublist in coordinates]


def parse_parcel_feature(parcel_feature: Feature) -> Optional[Tuple[str, Dict[str, Union[str, int]]]]:
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


def load_parcels(parcel_shapes_file_name: str) -> Dict[str, Dict[str, Union[str, int]]]:
    parcel_data = {}

    pool = mp.Pool()

    with fiona.open(parcel_shapes_file_name) as features:
        for result in pool.imap_unordered(parse_parcel_feature, features, chunksize=1000):
            if result is not None:
                parcel_data[result[0]] = result[1]
                if len(parcel_data) % 10000 == 0:
                    print(f"Loaded {str(len(parcel_data))} parcels...")

    pool.terminate()
    pool.join()

    return parcel_data


def parse_building_feature(building_feature: Feature) -> Polygon:
    building_feature["geometry"]["coordinates"] = clean_coordinates(building_feature["geometry"]["coordinates"])
    building_shape = geometry.shape(building_feature["geometry"])
    building_shape = ops.transform(PROJECT_FRANKLIN, building_shape)
    return building_shape


def load_buildings(building_shapes_file_name: str) -> List[Polygon]:
    building_shapes = []

    pool = mp.Pool()

    with fiona.open(building_shapes_file_name) as features:
        for result in pool.imap_unordered(parse_building_feature, features, chunksize=1000):
            building_shapes.append(result)

            if len(building_shapes) % 10000 == 0:
                print("Loaded " + str(len(building_shapes)) + " building shapes...")

    pool.terminate()
    pool.join()

    return building_shapes


def find_matching_parcel_wrapper(args):
    def find_matching_parcel(building_shape: Polygon, parcel_shape_tree: STRtree) -> Tuple[MatchStatus, Polygon, Optional[str]]:
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


def match_buildings_to_parcels(parcel_data: Dict[str, Dict[str, Union[str, int]]], building_shapes: List[Polygon]) -> List[Feature]:
    non_matches = 0
    partial_matches = 0
    full_matches = 0

    parcel_shapes = [wkt.loads(wkt_string) for wkt_string in parcel_data.keys()]
    parcel_shape_tree = STRtree(parcel_shapes)

    building_features = []

    pool = mp.Pool()

    for result in pool.imap_unordered(find_matching_parcel_wrapper,
                                      zip(building_shapes, itertools.repeat(parcel_shape_tree)),
                                      chunksize=math.ceil(len(building_shapes) / 8)):
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
            print(
                f"Matched {str(len(building_features))} buildings to parcel features! (F: {str(full_matches)}, P: {str(partial_matches)}, N: {str(non_matches)})")

    pool.terminate()
    pool.join()

    return building_features


def load_osu_building_ages():
    def get_building_age(building_number):
        response = requests.get(f"{OSU_BUILDING_DETAILS_ENDPOINT}/{building_number}")
        build_year_group = re.search('"Date Constructed":"(\\d*)/', response.text)
        if build_year_group is None:
            return 0
        else:
            return int(build_year_group.group(1))

    if os.path.isfile(OSU_BUILDING_AGES_FILE):
        with open(OSU_BUILDING_AGES_FILE, "r") as f:
            building_ages = json.load(f)
            print(f"Loaded {len(building_ages)} OSU building ages from {OSU_BUILDING_AGES_FILE}...")
            return building_ages
    else:
        building_ages = {}
        with fiona.open(OSU_BUILDING_DATA_FILE) as features:
            building_numbers = [str(feature["properties"]["BLDG_NUM"]) for feature in features]

            # Not parallelized to avoid rate-limiting
            for num in building_numbers:
                if num != "None" and num != "0" and num != "x":
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

        building_year = building_ages.get(str(feature["properties"]["BLDG_NUM"]))
        if building_year is None or building_year == 0:
            # The Franklin county data already has outlines of every building, so no point in leaving in
            # buildings with no year here
            return None
        else:
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
        for result in pool.imap_unordered(parse_osu_building_feature_wrapper,
                                          zip(features, itertools.repeat(building_ages)), chunksize=200):
            if result is not None:
                building_features.append(result)
                if len(building_features) % 100 == 0:
                    print(f"Loaded {len(building_features)} OSU building features...")

    pool.terminate()
    pool.join()

    # Buildings that didn't have a year assigned were returned as None and should be removed
    return building_features


def divide_features_by_dated_status(features: Iterable[Feature]) -> Tuple[List[Feature], List[Feature]]:
    dated_building_features = []
    undated_building_features = []

    for feature in features:
        if feature["properties"]["year_built"] == 0:
            undated_building_features.append(feature)
        else:
            dated_building_features.append(feature)

        if (len(dated_building_features) + len(undated_building_features)) % 5000 == 0:
            print(f"Divided {len(dated_building_features) + len(undated_building_features)} building features...")

    return dated_building_features, undated_building_features


# Returns None is the feature intersects with a dated building, the feature otherwise
def intersects_with_dated_wrapper(args) -> Optional[Feature]:
    def intersects_with_dated(undated_feature: Feature, dated_shape_tree: STRtree) -> Optional[Feature]:
        undated_building_shape = geometry.shape(undated_feature["geometry"])
        close_dated_shapes = dated_shape_tree.query(undated_building_shape.buffer(0.001))

        undated_feature_shape_prep = prep(undated_building_shape)

        for close_dated_shape in close_dated_shapes:
            if undated_feature_shape_prep.intersects(close_dated_shape):
                return None

        return undated_feature

    return intersects_with_dated(args[0], args[1])


def filter_intersecting_undated_buildings(dated_features: List[Feature], undated_features: List[Feature]) -> List[Feature]:
    dated_building_shapes = [geometry.shape(feature["geometry"]) for feature in dated_features]
    dated_shape_tree = STRtree(dated_building_shapes)

    num_intersecting_undateds = 0
    non_intersecting_undateds = []

    pool = mp.Pool()

    for feature in pool.imap_unordered(intersects_with_dated_wrapper,
                                       zip(undated_features, itertools.repeat(dated_shape_tree)),
                                       chunksize=math.ceil(len(undated_features) / 8)):
        if feature is None:
            num_intersecting_undateds += 1
        else:
            non_intersecting_undateds.append(feature)

        if (len(non_intersecting_undateds) + num_intersecting_undateds) % 1000 == 0:
            print(f"Filtered out {num_intersecting_undateds} undated buildings that intersect with a dated building... (Parsed {len(non_intersecting_undateds) + num_intersecting_undateds} undated buildings total...")

    pool.terminate()
    pool.join()

    return non_intersecting_undateds


def download_franklin_county_building_footprints(data_dir: str) -> str:
    with FTP('apps.franklincountyauditor.com') as ftp:
        ftp.login()
        ftp.cwd('GIS_Shapefiles')
        ftp.cwd('CurrentExtracts')

        building_footprint_file_name = next(file_name for file_name in ftp.nlst() if 'BuildingFootprints' in file_name)
        output_folder = f'{data_dir}/{building_footprint_file_name}'.replace('.zip', '')

        # Only downloads if the unzipped contents don't exist already
        if not Path(output_folder).is_dir():
            with tempfile.TemporaryDirectory() as temp_dir_name:
                zip_file_name = f'{temp_dir_name}/{building_footprint_file_name}'
                with open(zip_file_name, 'wb') as fp:
                    ftp.retrbinary(f'RETR {building_footprint_file_name}', fp.write)
                with ZipFile(zip_file_name) as zip_ref:
                    zip_ref.extractall(output_folder)

    return output_folder


def download_franklin_county_parcel_polygons(data_dir: str) -> str:
    with FTP('apps.franklincountyauditor.com') as ftp:
        ftp.login()
        ftp.cwd('GIS_Shapefiles')
        ftp.cwd('CurrentExtracts')

        parcel_polygons_file_name = next(file_name for file_name in ftp.nlst() if 'Parcel_Polygons' in file_name)
        output_folder = f'{data_dir}/{parcel_polygons_file_name}'.replace('.zip', '')

        # Only downloads if the unzipped contents don't exist already
        if not Path(output_folder).is_dir():
            with tempfile.TemporaryDirectory() as temp_dir_name:
                zip_file_name = f'{temp_dir_name}/{parcel_polygons_file_name}'
                with open(zip_file_name, 'wb') as fp:
                    ftp.retrbinary(f'RETR {parcel_polygons_file_name}', fp.write)
                with ZipFile(zip_file_name) as zip_ref:
                    zip_ref.extractall(output_folder)

    return output_folder


def main():
    start_time = datetime.now()

    data_dir = './data'
    os.makedirs(data_dir, exist_ok=True)

    osu_building_features = load_osu_building_features()

    # Download in parallel
    with ThreadPoolExecutor(2) as executor:
        future_footprint_dir_name = executor.submit(download_franklin_county_building_footprints, data_dir)
        future_parcels_dir_name = executor.submit(download_franklin_county_parcel_polygons, data_dir)
        # future_osu_data_dir_name = executor.submit(download_osu_data, data_dir) TODO

        timeout = 300
        footprint_dir_name = future_footprint_dir_name.result(timeout)
        parcels_dir_name = future_parcels_dir_name.result(timeout)
        # osu_data_dir_name = future_osu_data_file_name.result(timeout) TODO

    footprint_file_name = f'{footprint_dir_name}/BUILDINGFOOTPRINT.shp'
    parcels_file_name = f'{parcels_dir_name}/TAXPARCEL_CONDOUNITSTACK_LGIM.shp'

    franklin_building_shapes = load_buildings(footprint_file_name)
    franklin_parcel_data = load_parcels(parcels_file_name)
    franklin_building_features = match_buildings_to_parcels(franklin_parcel_data, franklin_building_shapes)

    all_building_features = itertools.chain(osu_building_features, franklin_building_features)
    dated_building_features, undated_building_features = divide_features_by_dated_status(all_building_features)

    undated_building_features = filter_intersecting_undated_buildings(dated_building_features,
                                                                      undated_building_features)
    print(f"Keeping {len(undated_building_features)} buildings without dates...")

    final_building_features = dated_building_features + undated_building_features

    print(f"Going to dump {len(final_building_features)} building features to {GEOJSON_OUT}...")
    with open(GEOJSON_OUT, "w") as file:
        geojson.dump(geojson.FeatureCollection(final_building_features), file)

    subprocess.call(["bash", "tippecanoe_cmd.sh", MBTILES_OUT, GEOJSON_OUT], stderr=sys.stderr, stdout=sys.stdout)
    print("Done! (Total time: " + str(datetime.now() - start_time) + ")")


if __name__ == '__main__':
    main()
