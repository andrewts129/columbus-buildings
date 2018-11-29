from shapely import geometry
import ijson.backends.yajl2_cffi as ijson
from collections import namedtuple
import csv
import geojson
import json
from geojson import Feature, FeatureCollection, Polygon

OHIO_BUILDING_SHAPES_FILE = "data/Ohio.geojson"
COUNTY_SHAPES_FILE = "data/gz_2010_us_050_00_500k.json"
BUILDING_AGES_FILE = "data/ColumbusBuildingAges.csv"

PointWithAge = namedtuple("PointWithAge", "position age")
BuildingWithAge = namedtuple("BuildingWithAge", "shape age")


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


def load_building_ages():
    result = []

    with open(BUILDING_AGES_FILE, "r") as file:
        reader = csv.reader(file)

        # Removes header row
        reader.__next__()

        for row in reader:
            latitude = row[3]
            longitude = row[4]
            year = row[5]

            if len(year) > 0 and len(latitude) > 0 and len(latitude) > 0:
                position = geometry.Point(float(longitude), float(latitude))
                result.append(PointWithAge(position, int(year)))

            if len(result) % 500 == 0:
                print("Loaded " + str(len(result)) + " building point ages...")

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


def main():
    franklin_shape = get_franklin_county_shape()
    print("Loaded Franklin County shape...")
    point_ages = load_building_ages()
    print("Loaded all (" + str(len(point_ages)) + ") building point ages...")
    franklin_building_shapes = get_franklin_co_buildings_shapes(franklin_shape)
    print("Loaded all (" + str(len(franklin_building_shapes)) + ") Franklin County building shapes...")
    buildings_with_ages = tag_buildings(franklin_building_shapes, point_ages)
    print("Tagged all (" + str(len(buildings_with_ages)) + ") building shapes with ages...")

    with open("age_tagged_building_shapes.json", "w") as file:
        geojson.dump(tagged_buildings_to_geojson(buildings_with_ages), file)

    print("Done!")


if __name__ == '__main__':
    main()
