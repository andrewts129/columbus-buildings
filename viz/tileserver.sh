#!/usr/bin/env bash

docker run --rm -it -v $(pwd):/data -p 8001:80 klokantech/tileserver-gl --verbose