import json
from shapely import geometry
import ijson

OHIO_BUILDING_SHAPES_FILE = "data/Ohio.geojson"
COUNTY_SHAPES_FILE = "data/gz_2010_us_050_00_500k.json"


def get_franklin_county_shape():
    with open(COUNTY_SHAPES_FILE, "r", encoding="ISO-8859-1") as file:
        counties = json.load(file)

    for feature in counties["features"]:
        if feature["properties"]["NAME"] == "Franklin" and feature["properties"]["STATE"] == "39":
            return geometry.shape(feature["geometry"])


def get_franklin_co_buildings_shapes(franklin_county_shape):
    result = []

    with open(OHIO_BUILDING_SHAPES_FILE, "r") as file:
        building_features = ijson.items(file, "features.item")

        for feature in building_features:
            building_shape = geometry.shape(feature["geometry"])
            if franklin_county_shape.contains(building_shape):
                result.append(building_shape)

    return result


def main():


if __name__ == '__main__':
    main()
