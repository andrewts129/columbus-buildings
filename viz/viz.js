window.onload = function() {
    if (!mapboxgl.supported()) {
        window.alert("Sorry, your browser doesn't support the map on this page")
    }
    else {
        const tileServerURL = "http://localhost:8080";

        let map = new mapboxgl.Map({
            container: 'map',
            center: [-82.9988, 39.9612],
            zoom: 11,
            minZoom: 11,
            maxZoom: 16,
            style: tileServerURL + "/styles/base/style.json",
            antialias: true,
        });

        map.on("load", function () {
            map.addSource("buildings", {
                id: "buildings",
                type: "vector",
                tiles:[tileServerURL + "/data/data/{z}/{x}/{y}.pbf"],
                minzoom: 11,
                maxzoom: 15,
            });

            map.addSource("columbus", {
                id: "columbus",
                type: "vector",
                tiles:[tileServerURL + "/data/columbus/{z}/{x}/{y}.pbf"]
            });

            map.addControl(new mapboxgl.FullscreenControl());

            const colors = ["#e41a1c", "#f24d0e", "#ff7f00", "#FFBF1A", "#ffff33", "#A6D73F", "#4daf4a", "#429781", "#377eb8", "#6866AE", "#984ea3"];
            const stopYears = [1800, 1825, 1850, 1875, 1900, 1925, 1950, 1975, 2000, 2025, 2050];

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
                            return [stopYears[index], color]
                        })
                    }
                }
            });

            map.addLayer({
                id: undated_building_layer_id,
                type: "fill",
                source: "buildings",
                "source-layer": "data",
                filter: ["==", "year_built", unavailable_year],
                paint: {
                    "fill-color": unavailable_color
                }
            }, "dated_buildings");
        })
    }
};
