#!/usr/bin/env bash
pig -x local -p IN=data/admin0_MX.geojson -p OUT=mx_test/ tile_geometries.pig
cut -f2 mx_test/* | ./features_to_collection.sh > mexico_tiles_Z11.geojson
