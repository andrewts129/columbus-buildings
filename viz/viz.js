window.onload = function() {
    const map = L.map('map').setView([39.9612, -82.9988], 13);

    L.tileLayer('https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token={accessToken}', {
        attribution: 'Map data &copy; <a href="https://www.openstreetmap.org/">OpenStreetMap</a> contributors, <a href="https://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="https://www.mapbox.com/">Mapbox</a>',
        maxZoom: 18,
        id: 'mapbox.streets',
        accessToken: 'pk.eyJ1IjoiYW5kcmV3dHMxMjkiLCJhIjoiY2pxMThvdXVrMHYyaDN4cDVjNnQ5Z20zMCJ9.ikQ0fMhcTLX6ILtiYdBLZg'
    }).addTo(map);

    L.tileLayer('Data/{z}/{x}/{y}.png').addTo(map);

    //const mb = L.tileLayer.mbTiles('data.mbtiles').addTo(map);
    //console.log(mb)
};