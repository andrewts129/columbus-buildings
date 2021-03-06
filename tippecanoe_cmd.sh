#!/usr/bin/env bash
tippecanoe -o $1 --minimum-zoom=11 --maximum-zoom=15 --include=year_built --read-parallel --force $2
echo "Size of $1 = $(($(stat -c%s "$1") / 1000000)) MB."