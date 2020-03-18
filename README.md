# Vector Tile Generator & Server for the Columbus Building Age Map  
  
This repository contains a script `GenerateTiles.py`, that downloads building data 
from the [Franklin County Auditor](ftp://apps.franklincountyauditor.com/) and uses it
to build a vector tileset using [tippecanoe](https://github.com/mapbox/tippecanoe/).  
  
The script also uses data from OSU to get construction dates for their buildings. Right now,
it doesn't automatically download building footprints from OSU, you have to download it manually 
from [here](https://gismaps.osu.edu/OSUMaps/Default.html?#) into `data/OhioState/data.gdb`.  
  
The `tileserver` folder is used to create a Docker image containing the vector tiles and a
[tile server](https://github.com/maptiler/tileserver-gl).
 
The `viz` folder is only for local testing. To see the actual front-end code for the map, see the [repository
of the website hosting the map](https://github.com/andrewts129/andrew-smith-dot-io).