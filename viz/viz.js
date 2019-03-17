window.onload = function() {
    //mapboxgl.accessToken = "pk.eyJ1IjoiYW5kcmV3dHMxMjkiLCJhIjoiY2pxMThudjRiMHYwMjQ1c3pjMDlqYWVteiJ9.IMFflTD9AA78V9-5JQ-HeQ"

    let map = new mapboxgl.Map({
        container: 'map',
        center: [-82.9988, 39.9612],
        zoom: 12,
        minZoom: 12,
        maxZoom: 18,
        style: "http://localhost:8080/styles/base/style.json"
    });

    map.on("load", function () {
        map.addSource("columbus", {
            id: "columbus",
            type: "vector",
            tiles:["http://localhost:8080/data/columbus/{z}/{x}/{y}.pbf"]
        });

        map.addSource("buildings", {
            id: "buildings",
            type: "vector",
            tiles:["http://localhost:8080/data/data/{z}/{x}/{y}.pbf"],
            minzoom: 12,
            maxzoom: 15,
        });

        const min_year = 1800;
        const max_year = 2019;
        const colors = ["#e41a1c", "#ff7f00", "#ffff33", "#4daf4a", "#377eb8"];

        map.addLayer({
            id: "buildings",
            type: "fill",
            source: "buildings",
            "source-layer": "data",
            paint: {
                "fill-color": {
                    "property": "year_built",
                    "stops": [[0, "#eaeae5"]].concat(colors.map(function (color, index) {
                        return [min_year + (index * (max_year - min_year) / (colors.length - 1)), color]
                    }))
                }
            }
        })
    })
};
