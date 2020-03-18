#!/usr/bin/env bash
docker run -it -v "$(pwd)"/data:/data -p 8001:80 maptiler/tileserver-gl:v3.0.0
