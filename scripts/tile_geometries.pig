register '../target/lib/gt-main-2.7.0.jar';
register '../target/lib/gt-geojson-2.7.0.jar';
register '../target/lib/jts-1.8.jar';
register '../target/lib/json-simple-1.1.1.jar'; 
register '../target/kvadrere-0.0.1.jar';

%default ZOOM 11

geometries = LOAD '$IN' AS (json:chararray);
slices = FOREACH geometries GENERATE FLATTEN(kvadrere.pig.geo.RecursivelyTileGeometry($ZOOM,json)) AS (json:chararray);
STORE slices into '$OUT';
