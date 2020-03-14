#!/usr/bin/env python3
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
from concurrent.futures.thread import ThreadPoolExecutor
from ftplib import FTP
from pathlib import Path
from typing import Iterable, Hashable, Dict
from zipfile import ZipFile

import fiona
import geopandas as gpd
import pandas as pd
import requests
from geopandas import GeoDataFrame

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)


def download_franklin_county_building_footprints(data_dir: str) -> str:
    with FTP('apps.franklincountyauditor.com') as ftp:
        ftp.login()
        ftp.cwd('GIS_Shapefiles')
        ftp.cwd('CurrentExtracts')

        building_footprint_file_name = next(file_name for file_name in ftp.nlst() if 'BuildingFootprints' in file_name)
        output_folder = f'{data_dir}/{building_footprint_file_name}'.replace('.zip', '')

        # Only downloads if the unzipped contents don't exist already
        if not Path(output_folder).is_dir():
            logger.debug(f'Downloading {building_footprint_file_name}...')
            with tempfile.TemporaryDirectory() as temp_dir_name:
                zip_file_name = f'{temp_dir_name}/{building_footprint_file_name}'
                with open(zip_file_name, 'wb') as fp:
                    ftp.retrbinary(f'RETR {building_footprint_file_name}', fp.write)
                with ZipFile(zip_file_name) as zip_ref:
                    zip_ref.extractall(output_folder)

    logger.debug(f'Using {building_footprint_file_name}...')
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
            logger.debug(f'Downloading {parcel_polygons_file_name}...')
            with tempfile.TemporaryDirectory() as temp_dir_name:
                zip_file_name = f'{temp_dir_name}/{parcel_polygons_file_name}'
                with open(zip_file_name, 'wb') as fp:
                    ftp.retrbinary(f'RETR {parcel_polygons_file_name}', fp.write)
                with ZipFile(zip_file_name) as zip_ref:
                    zip_ref.extractall(output_folder)

    logger.debug(f'Using {parcel_polygons_file_name}...')
    return output_folder


def select_keys(d: Dict, keys_to_keep: Iterable[Hashable]) -> Dict:
    return {k: v for k, v in d.items() if k in keys_to_keep}


def features_slimmed(fiona_reader: Iterable, properties_to_keep: Iterable[str]) -> Iterable:
    for feature in fiona_reader:
        new_feature = feature.copy()
        new_feature['properties'] = select_keys(new_feature['properties'], properties_to_keep)
        yield new_feature


def load_footprints(footprint_file_name: str) -> GeoDataFrame:
    with fiona.open(footprint_file_name) as features:
        properties_to_keep = []  # None of the properties here are useful
        df = GeoDataFrame.from_features(features_slimmed(features, properties_to_keep))
        df.crs = features.crs

    return df.to_crs(epsg=4326)


def load_parcels(parcel_file_name: str) -> GeoDataFrame:
    with fiona.open(parcel_file_name) as features:
        properties_to_keep = ['PARCELID', 'RESYRBLT']
        slim_features = features_slimmed(features, properties_to_keep)  # Reduces memory usage
        slim_features = (f for f in slim_features if
                         f['geometry'] is not None)  # Apparently there are things in here with no shape?
        df = GeoDataFrame.from_features(slim_features)
        df.crs = features.crs

    return df.to_crs(epsg=4326)


def download_osu_buildings_ages(building_file_name: str, data_dir: str) -> GeoDataFrame:
    def fetch_building_age(building_number: str) -> int:
        response = requests.get(
            f"https://gismaps.osu.edu/OSUDataService/OSUService.svc/BuildingDetailsExtended/{building_number}")
        build_year_group = re.search('"Date Constructed":"(\\d*)/', response.text)
        if build_year_group is None:
            return 0
        else:
            return int(build_year_group.group(1))

    age_cache_file_name = f"{data_dir}/OhioState/ages.json"

    if os.path.isfile(age_cache_file_name):
        with open(age_cache_file_name, "r") as f:
            building_ages = json.load(f)
            logging.debug(f"Loaded {len(building_ages)} OSU building ages from cache at {age_cache_file_name}...")
    else:
        building_ages = {}
        with fiona.open(building_file_name) as features:
            building_numbers = [str(feature["properties"]["BLDG_NUM"]) for feature in features]

            # Not parallelized to avoid rate-limiting
            for num in building_numbers:
                if num != "None" and num != "0" and num != "x":
                    building_ages[num] = fetch_building_age(num)
                    logging.debug(f"Loaded {len(building_ages)} OSU building ages from https://gismaps.osu.edu...")

        logging.debug(f"Writing {len(building_ages)} OSU building ages to cache at {age_cache_file_name}...")
        with open(age_cache_file_name, "w") as f:
            json.dump(building_ages, f)

    return gpd.GeoDataFrame(building_ages.items(), columns=['BLDG_NUM', 'year_built'])


def load_osu_buildings(building_file_name: str, data_dir: str) -> GeoDataFrame:
    with fiona.open(building_file_name) as features:
        df = GeoDataFrame.from_features(features)
        df.crs = features.crs

    building_ages = download_osu_buildings_ages(building_file_name, data_dir)
    df = df.merge(building_ages, on='BLDG_NUM')
    df = df[['geometry', 'year_built']]  # Remove all the columns we don't need

    return df.to_crs(epsg=4326)


def contains_letters(s: str) -> bool:
    return any(c.isalpha() for c in s)


def sane_year_built(year: int) -> bool:
    return 1776 <= int(year) <= 2027


def clean_parcel_id(s: str) -> str:
    if s[-3:] == '-00':
        return s[:-3].strip().replace('-', '')
    else:
        return s.strip().replace('-', '')


def clean_parcel_data_frame(df: GeoDataFrame) -> GeoDataFrame:
    new_df = df.rename(columns={'PARCELID': 'parcel_id', 'RESYRBLT': 'year_built'})

    new_df = new_df[~new_df.parcel_id.apply(contains_letters)]
    new_df.parcel_id = new_df.parcel_id.apply(clean_parcel_id)
    new_df = new_df[new_df.year_built.apply(sane_year_built)]

    return new_df


def main():
    data_dir = './data'
    os.makedirs(data_dir, exist_ok=True)

    # Download in parallel
    with ThreadPoolExecutor(3) as executor:
        future_osu_buildings = executor.submit(load_osu_buildings, f'{data_dir}/OhioState/data.gdb', data_dir)
        future_footprint_dir_name = executor.submit(download_franklin_county_building_footprints, data_dir)
        future_parcels_dir_name = executor.submit(download_franklin_county_parcel_polygons, data_dir)

        timeout = 300
        osu_buildings = future_osu_buildings.result(timeout)
        footprint_dir_name = future_footprint_dir_name.result(timeout)
        parcels_dir_name = future_parcels_dir_name.result(timeout)

    logger.info('Downloaded data...')

    logger.debug(osu_buildings.head())
    logger.debug(osu_buildings.info())
    logger.debug(osu_buildings.describe())

    footprint_file_name = f'{footprint_dir_name}/BUILDINGFOOTPRINT.shp'
    parcels_file_name = f'{parcels_dir_name}/TAXPARCEL_CONDOUNITSTACK_LGIM.shp'

    footprints = load_footprints(footprint_file_name)
    logger.debug(footprints.head())
    logger.debug(footprints.info())

    parcels = clean_parcel_data_frame(load_parcels(parcels_file_name))
    logger.debug(parcels.head())
    logger.debug(parcels.info())
    logger.debug(parcels.describe())

    logger.info('Loaded data...')

    footprints_with_years = gpd.sjoin(footprints, parcels, op='intersects', how='left')
    logger.debug(footprints_with_years.head())
    logger.debug(footprints_with_years.info())
    logger.debug(footprints_with_years.describe())

    final_df = GeoDataFrame(pd.concat([footprints_with_years, osu_buildings], ignore_index=True),
                            crs=footprints_with_years.crs)
    final_df = final_df[['geometry', 'year_built']]  # Remove all the columns we don't need
    logger.debug(final_df.head())
    logger.debug(final_df.info())
    logger.debug(final_df.describe())

    logger.info('Joined data...')

    # TODO do something about duplicate buildings
    output_geojson_file_name = f'{data_dir}/buildings.geojson'
    output_mbtiles_file_name = f'{data_dir}/buildings.mbtiles'
    final_df.to_file(output_geojson_file_name, driver='GeoJSON')
    subprocess.call(['bash', 'tippecanoe_cmd.sh', output_mbtiles_file_name, output_geojson_file_name],
                    stderr=sys.stderr, stdout=sys.stdout)

    logging.info('done!')


if __name__ == '__main__':
    main()
