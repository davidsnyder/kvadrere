/*
 * Copyright 2012 David Snyder
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package kvadrere.pig.geo;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;

import org.geotools.geojson.geom.GeometryJSON;
       
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.TopologyException;

import org.json.simple.JSONValue;
import org.json.simple.JSONObject;

/*
  @params zoomLevel (Integer) 1-23
  @params geoJSON Feature (String) Point or Polygon

  For a geoJSON point, the UDF will just compute the quadkey and return (quadkey,point) where point is the geoJSON Feature with a new attached property 'quadkey'.

  For a geoJSON polygon, the UDF will slice the shape on tile boundaries, yielding (quadkey,polygon_slice) for each portion,
  where polygon_slice is a subgeometry of the parameter with regurgitated properties from its parent, and a new property 'quadkey'.

  RecursivelyTileGeometry returns the same result as TileGeometry, but tiles the polygon from 1 up to zoomLevel, which allows it to save computation because
  if a larger tile at a lower zoom level does not contain any portion of the polygon, the procedure can exit early without wasting time computing all the smaller tiles at the provided zoomLevel.

  If the polygon fills a large portion of its bounding box, this procedure may result in extra work.
*/  

public class RecursivelyTileGeometry extends EvalFunc<DataBag> {

  private static TupleFactory tupleFactory = TupleFactory.getInstance();
  private static BagFactory   bagFactory   = BagFactory.getInstance();
  private static GeometryJSON gjson        = new GeometryJSON();    
  private static final String GEOM_POINT   = "Point";
  private static final String GEOM_COLLEC   = "GeometryCollection";  
  private static final String GEOM_POLYGON  = "Polygon";
  private static final String GEOM_MULTIPOLYGON  = "MultiPolygon";  

  public DataBag exec(Tuple input) throws IOException {
    if (input == null || input.size() < 2 || input.isNull(0) || input.isNull(1))
      return null;

    DataBag returnKeys = bagFactory.newDefaultBag();
        
    // Extract function parameters        
    String zlvl   = input.get(0).toString();
    int zoomLevel = Integer.parseInt(zlvl);
    String jsonBlob = input.get(1).toString();

    JSONObject jsonObject = parseJSONString(jsonBlob);

    Reader reader = new StringReader(jsonObject.get("geometry").toString());    
    Geometry geom = gjson.read(reader);
    
    if(zoomLevel < 1 || zoomLevel > 23) return null; //zoomLevel exceeds bounds

    if(geom.isEmpty()) return null; // Stop if the geometry is empty            

    try {
      
      if (geom.getGeometryType().equals(GEOM_POINT)) { // Point

        String quadkey = QuadKeyUtils.geoPointToQuadKey(((Point)geom).getX(), ((Point)geom).getY(), zoomLevel);
        JSONObject newJSONObject = setGeometry(geom,jsonObject);

        Tuple newQuadKey = tupleFactory.newTuple(2);        
        newQuadKey.set(0, quadkey);
        newQuadKey.set(1, newJSONObject.toString());
        returnKeys.add(newQuadKey);
        
      } else { // Polygon or MultiPolygon
        
        // getEnvelope() returns a Polygon whose points are (minx, miny), (maxx, miny), (maxx, maxy), (minx, maxy), (minx, miny).              
        Polygon boundingBox = (Polygon)geom.getEnvelope();
        int startZoom = 1;        
        Coordinate [] boxTileCoords = QuadKeyUtils.latLngToTileCoords(boundingBox.getCoordinates(),startZoom);

        // Increase startZoom as much as possible until either the max zoom is reached, or the bounding box no longer fits within a single tile.
        while((startZoom < zoomLevel) && (((int)boxTileCoords[0].x == (int)boxTileCoords[2].x) || ((int)boxTileCoords[0].y == (int)boxTileCoords[2].y))) {
          startZoom += 1;
          boxTileCoords = QuadKeyUtils.latLngToTileCoords(boundingBox.getCoordinates(),startZoom);          
        }

        Coordinate [] boxCoordinates = boundingBox.getCoordinates();
        // These are going to be tile coordinates, not lat/long coordinates.
        Coordinate [] boxTileCoordinates = QuadKeyUtils.latLngToTileCoords(boxCoordinates,startZoom);

        double maxX = boxTileCoordinates[0].x;
        double minX = boxTileCoordinates[0].x;
        double maxY = boxTileCoordinates[0].y;
        double minY = boxTileCoordinates[0].y;
        
        for(Coordinate c : boxTileCoordinates) {
          if(c.x > maxX) maxX = c.x;
          if(c.y > maxY) maxY = c.y;
          if(c.x < minX) minX = c.x;
          if(c.y < minY) minY = c.y;      
        }
    
        for(int tileX = (int)minX; tileX <= (int)maxX; tileX++) {
          for(int tileY = (int)minY; tileY <= (int)maxY; tileY++) {
            String quadkey = QuadKeyUtils.tileXYToQuadKey(tileX,tileY,startZoom);
            // Recursively search for tiles that contain portions of the provided geometry
            for(Tuple t: searchTile(quadkey,geom,jsonObject,zoomLevel)) {
              returnKeys.add(t);
            }
          }
        }
      }
    } catch (TopologyException e) { System.out.println(e.getMessage()); throw e;}
    return returnKeys;
  }

  public DataBag searchTile(String quadkey,Geometry shape,JSONObject properties,int maxZoom) throws IOException {
    DataBag polygonSlices = bagFactory.newDefaultBag();
    List<String> childKeys;
    
    if(quadkey.length() == maxZoom) { // If the geometry's bounding box fits entirely within a single tile, then there's no children to grab, just stop and yield this tile
      childKeys = new ArrayList<String>();
      childKeys.add(quadkey);
    } else {
      childKeys = QuadKeyUtils.childrenFor(quadkey);
      for(String childKey : childKeys) {
        Polygon tileBox = QuadKeyUtils.quadKeyToBox(childKey);
        Geometry polygonSlice = tileBox.intersection(shape);
        polygonSlice = ( polygonSlice.getGeometryType().equals(GEOM_COLLEC) ? polygonSlice.getEnvelope() : polygonSlice );
        
        if(polygonSlice.getArea() > 0.0) { // Skip empty punchout polygons (since our bounding box could easily have uncovered tiles)
          if(childKey.length() == maxZoom) { // We are done, we have found a tile at our desired maxZoom that contains a portion of the initial polygon. Add this polygonSlice to the bag and yield it up.
            JSONObject newProperties = setGeometry(polygonSlice,properties);
            Tuple newQuadKey = tupleFactory.newTuple(2);        
            newQuadKey.set(0, quadkey);
            newQuadKey.set(1,newProperties.toString());
            polygonSlices.add(newQuadKey);
          }
          else {
            for(Tuple tuple : searchTile(childKey,polygonSlice,properties,maxZoom)) {
              polygonSlices.add(tuple);
            }
          }
        }
      }
    }
    return polygonSlices;
  }

  public JSONObject setGeometry(Geometry geom, JSONObject object) throws IOException {
    GeometryJSON gjson = new GeometryJSON();    
    StringWriter writer = new StringWriter();

    gjson.write(geom,writer);
    JSONObject jsonGeometry = parseJSONString(writer.toString());                    
        
    object.put("geometry",jsonGeometry);
    
    return object;
  }

  public JSONObject parseJSONString(String jsonBlob) {
    Reader reader = new StringReader(jsonBlob);
    Object jsonObject = JSONValue.parse(reader);
    return (JSONObject) jsonObject;
  }

}
