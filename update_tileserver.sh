#!/usr/bin/env bash
source venv/bin/activate
./GenerateTiles.py
cp data/buildings.mbtiles tileserver/buildings.mbtiles
cd tileserver || exit
./deploy.sh