window.onload = function() {
    let map = new mapboxgl.Map({
        container: 'map',
        center: [-82.9988, 39.9612],
        zoom: 12,
        minZoom: 12,
        maxZoom: 18,
        style: "http://localhost:8080/styles/base/style.json"
    });

    map.on("load", function () {
        map.addSource("buildings", {
            id: "buildings",
            type: "vector",
            tiles:["http://localhost:8080/data/data/{z}/{x}/{y}.pbf"],
            minzoom: 12,
            maxzoom: 15,
        });

        map.addSource("columbus", {
            id: "columbus",
            type: "vector",
            tiles:["http://localhost:8080/data/columbus/{z}/{x}/{y}.pbf"]
        });

        const min_year = 1800;
        const max_year = 2019;
        const colors = ["#e41a1c", "#ff7f00", "#ffff33", "#4daf4a", "#377eb8"];

        const unavailable_year = 0;
        const unavailable_color = "#eaeae5";

        const dated_building_layer_id = "dated_buildings";
        const undated_building_layer_id = "undated_buildings";

        map.addLayer({
            id: dated_building_layer_id,
            type: "fill",
            source: "buildings",
            "source-layer": "data",
            filter: ["!=", "year_built", unavailable_year],
            paint: {
                "fill-color": {
                    "property": "year_built",
                    "stops": colors.map(function (color, index) {
                        return [min_year + (index * (max_year - min_year) / (colors.length - 1)), color]
                    })
                }
            }
        });

        map.addLayer({
            id: undated_building_layer_id,
            type: "fill",
            source: "buildings",
            "source-layer": "data",
            filter: ["==", "year_built", 0],
            paint: {
                "fill-color": unavailable_color
            }
        }, "dated_buildings");
    })
};
