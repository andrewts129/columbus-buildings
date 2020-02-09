import os
import tempfile
from concurrent.futures.thread import ThreadPoolExecutor
from ftplib import FTP
from pathlib import Path
from typing import Iterable, Hashable, Dict
from zipfile import ZipFile

import fiona
import geopandas as gpd
from geojson import Feature
from geopandas import GeoDataFrame


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

    df.to_crs(epsg=4326)
    return df


def load_parcels(parcel_file_name: str) -> GeoDataFrame:
    with fiona.open(parcel_file_name) as features:
        properties_to_keep = ['PARCELID', 'RESYRBLT']
        slim_features = features_slimmed(features, properties_to_keep)
        slim_features = (f for f in slim_features if f['geometry'] is not None)  # Apparently there are things in here with no shape
        df = GeoDataFrame.from_features(slim_features)
        df.crs = features.crs

    df.to_crs(epsg=4326)
    return df


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

    # Download in parallel TODO get osu data too
    with ThreadPoolExecutor(2) as executor:
        future_footprint_dir_name = executor.submit(download_franklin_county_building_footprints, data_dir)
        future_parcels_dir_name = executor.submit(download_franklin_county_parcel_polygons, data_dir)

        timeout = 300
        footprint_dir_name = future_footprint_dir_name.result(timeout)
        parcels_dir_name = future_parcels_dir_name.result(timeout)

    print('Downloaded data...')

    footprint_file_name = f'{footprint_dir_name}/BUILDINGFOOTPRINT.shp'
    parcels_file_name = f'{parcels_dir_name}/TAXPARCEL_CONDOUNITSTACK_LGIM.shp'

    footprints = load_footprints(footprint_file_name)
    parcels = clean_parcel_data_frame(load_parcels(parcels_file_name))

    print('Loaded data...')

    footprints_with_years = gpd.sjoin(footprints, parcels, op='within', how='left')

    print('Joined data...')

    print(footprints_with_years.head())
    print(footprints_with_years.describe())
    print('done!')


if __name__ == '__main__':
    main()
