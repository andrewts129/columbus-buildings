from shapely import geometry
import ijson.backends.yajl2_cffi as ijson
from collections import namedtuple
import csv
import geojson
import json
from geojson import Feature, FeatureCollection, Polygon
import fiona
import pyproj

BUILDING_AGES_FILE = "data/ColumbusBuildingAges.csv"
PARCEL_AGES_FILE = "data/ParcelAges.csv"
PARCEL_SHAPES_FILE = "data/20181101_Parcel_Polygons/TAXPARCEL_CONDOUNITSTACK_LGIM.shp"
BUILDING_SHAPES_FILE = "data/BuildingFootprints/BUILDINGFOOTPRINT.shp"

Parcel = namedtuple("Parcel", "number address year shape")
BuildingWithAge = namedtuple("BuildingWithAge", "shape age")

IN_PROJ = pyproj.Proj("+proj=lcc +lat_0=38 +lat_1=38.73333333 +lat_2=40.03333 +lon_0=-82.5 +x_0=600000 +y_0=0 +datum=NAD83 +units=us-ft +no_defs")
OUT_PROJ = pyproj.Proj(init="epsg:4326")


def _contains_letters(s):
    return any(c.isalpha() for c in s)


def _clean_parcel_id(s):
    if s[-3:] == "-00":
        return s[:-3].strip().replace("-", "")
    else:
        return s.strip().replace("-", "")


def load_parcel_shapes():
    def clean_coordinates(coordinates):
        if isinstance(coordinates, tuple):
            converted_coords = pyproj.transform(IN_PROJ, OUT_PROJ, coordinates[0], coordinates[1])
            return converted_coords[0], converted_coords[1]
        else:
            return [clean_coordinates(x) for x in coordinates]

    def create_polygon(parcel_feature):
        parcel_id = _clean_parcel_id(parcel_feature["properties"]["PARCELID"])

        if not _contains_letters(parcel_id) and parcel_feature["geometry"] is not None:
            return parcel_id, Polygon(clean_coordinates(parcel_feature["geometry"]["coordinates"]))
        else:
            return None

    parcel_shapes = [create_polygon(feature) for feature in fiona.open(PARCEL_SHAPES_FILE)]
    parcel_shapes = list(filter(lambda x: x is not None, parcel_shapes))

    print("Created all polygons (" + str(len(parcel_shapes)) + ")")

    result = dict(parcel_shapes)

    print("Mapped polygons to parcel IDs")
    return result


def tag_parcel_ages(parcel_shapes):
    features = []

    with open(PARCEL_AGES_FILE, "r") as file:
        reader = csv.reader(file)

        # Removes header row
        reader.__next__()

        for row in reader:
            parcel_id = _clean_parcel_id(row[0])
            address = row[1]
            year = row[2]

            if parcel_id is not None and len(year) > 0:
                parcel_shape = parcel_shapes.get(parcel_id)

                if parcel_shape is not None:
                    feature = Feature(geometry=parcel_shape, properties={
                        "parcel_id": parcel_id,
                        "address": address,
                        "year": int(year)})

                    features.append(feature)

                    # TODO remove this
                    if False and len(features) > 100:
                        return FeatureCollection(features)

                    if len(features) % 10000 == 0:
                        print("Tagged " + str(len(features)) + " parcels with years...")

    return FeatureCollection(features)


def main():
    parcel_shapes = load_parcel_shapes()
    parcels = tag_parcel_ages(parcel_shapes)

    with open("viz/data.geojson", "w") as file:
        geojson.dump(parcels, file)

    print("Done!")


if __name__ == '__main__':
    main()
