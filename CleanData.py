from shapely import geometry
import ijson.backends.yajl2_cffi as ijson
from collections import namedtuple
import csv
import geojson
import json
from geojson import Feature, FeatureCollection, Polygon
import fiona

OHIO_BUILDING_SHAPES_FILE = "data/Ohio.geojson"
COUNTY_SHAPES_FILE = "data/gz_2010_us_050_00_500k.json"
BUILDING_AGES_FILE = "data/ColumbusBuildingAges.csv"
PARCEL_AGES_FILE = "data/ParcelAges.csv"
PARCEL_SHAPES_FILE = "data/20181101_Parcel_Polygons/TAXPARCEL_CONDOUNITSTACK_LGIM.shp"

Parcel = namedtuple("Parcel", "number address year shape")
BuildingWithAge = namedtuple("BuildingWithAge", "shape age")


def _contains_letters(s):
    return any(c.isalpha() for c in s)


def _clean_parcel_id(s):
    if s[-3:] == "-00":
        return s[:-3].strip().replace("-", "")
    else:
        return s.strip().replace("-", "")


def get_franklin_county_shape():
    with open(COUNTY_SHAPES_FILE, "r", encoding="ISO-8859-1") as file:
        counties = json.load(file)

    for feature in counties["features"]:
        if feature["properties"]["NAME"] == "Franklin" and feature["properties"]["STATE"] == "39":
            return geometry.shape(feature["geometry"])


def get_franklin_co_buildings_shapes(franklin_county_shape):
    result = []

    with open(OHIO_BUILDING_SHAPES_FILE, "rb") as file:
        building_features = ijson.items(file, "features.item")

        for feature in building_features:
            building_shape = geometry.shape(feature["geometry"])
            if franklin_county_shape.contains(building_shape):
                result.append(building_shape)

            if len(result) % 500 == 0 and len(result) != 0:
                print("Loaded " + str(len(result)) + " Franklin County Building Ages...")

            if len(result) > 1000:
                break

    return result


def tag_buildings(building_shapes, point_ages):
    result = []

    for building_shape in building_shapes:
        for point_age in point_ages:
            if building_shape.contains(point_age.position):
                result.append(BuildingWithAge(building_shape, point_age.age))
                break

        print("Tagged " + str(len(result)) + " building shapes with ages...")

    return result


def tagged_buildings_to_geojson(tagged_buildings):
    features = [Feature(geometry=Polygon(x.shape), properties={"age": x.age}) for x in tagged_buildings]
    return FeatureCollection(features)


def load_parcel_shapes():
    def create_polygon(parcel_feature):
        parcel_id = _clean_parcel_id(parcel_feature["properties"]["PARCELID"])

        if not _contains_letters(parcel_id) and parcel_feature["geometry"] is not None:
            return parcel_id, Polygon(parcel_feature["geometry"]["coordinates"])
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

                    if len(features) % 10000 == 0:
                        print("Tagged " + str(len(features)) + " parcels with years...")

    return FeatureCollection(features)


def main():
    parcel_shapes = load_parcel_shapes()
    parcels = tag_parcel_ages(parcel_shapes)

    with open("parcels.geojson", "w") as file:
        geojson.dump(parcels, file)

    print("Done!")


if __name__ == '__main__':
    main()
