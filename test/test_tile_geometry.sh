#!/usr/bin/env bash
pig -x local -p IN=data/admin0_MX.geojson -p OUT=admin0_MX_tiled_Z11/ tile_geometries.pig
cut -f2 admin0_MX_tiled_Z11/* | ./features_to_collection.sh > mexico_tiles_Z11.geojson
