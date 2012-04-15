#!/usr/bin/env bash

echo -ne "{\"type\":\"FeatureCollection\", \"features\":["
read line
echo -ne "$line" #stupid fencepost problem
while read line; do
  echo -ne ",$line" 
done 
echo -ne "]}"
