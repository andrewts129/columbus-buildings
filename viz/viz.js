window.onload = function() {
    mapboxgl.accessToken = "pk.eyJ1IjoiYW5kcmV3dHMxMjkiLCJhIjoiY2pxMThudjRiMHYwMjQ1c3pjMDlqYWVteiJ9.IMFflTD9AA78V9-5JQ-HeQ"

    let map = new mapboxgl.Map({
        container: 'map',
        center: [39.9612, -82.9988],
        zoom: 9,
        style: 'mapbox://styles/mapbox/streets-v9'
    });

    map.on("load", function () {
        map.addSource("buildings", {
            id: "buildings",
            type: "vector",
            tiles:["http://localhost:8080/data/data/{z}/{x}/{y}.pbf"],
            minzoom: 0,
            maxzoom: 14
        });

        map.addLayer({
            id: "buildings",
            type: "fill",
            source: "buildings",
            "source-layer": "data",
            paint: {
                "fill-color": {
                    "property": "year_built",
                    "stops": [
                        [1800, "white"],
                        [2018, "steelblue"]
                    ]
                }
            }
        })
    })
};
