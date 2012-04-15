register '../target/kvadrere-0.0.1-SNAPSHOT-jar-with-dependencies.jar';

%default ZOOM 11

features = LOAD '$IN' AS (json:chararray);
tiles = FOREACH features GENERATE FLATTEN(kvadrere.pig.geo.TileGeometry($ZOOM,json)) AS (quadkey:chararray,json:chararray);
STORE tiles into '$OUT';
