register '../target/kvadrere-0.0.1-SNAPSHOT-jar-with-dependencies.jar';

%default ZOOM 11

geometries = LOAD '$IN' AS (json:chararray);
slices = FOREACH geometries GENERATE FLATTEN(kvadrere.pig.geo.TileGeometry($ZOOM,json)) AS (quadkey:chararray,json:chararray);
STORE slices into '$OUT';
