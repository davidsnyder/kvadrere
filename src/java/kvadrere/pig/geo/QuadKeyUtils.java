/*
 * Copyright 2011 Jacob Perkins
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

import java.util.Iterator;
import java.util.ArrayList;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;

import com.vividsolutions.jts.geom.*;

public final class QuadKeyUtils {

  private static final int TILE_SIZE = 256;
  private static final double MIN_LATITUDE = -85.05112878;
  private static final double MAX_LATITUDE = 85.05112878;
  private static final double MIN_LONGITUDE = -180;
  private static final double MAX_LONGITUDE = 180;
  private static final int MIN_ZOOM_LEVEL = 1;    
  private static final int MAX_ZOOM_LEVEL = 23;

  private static TupleFactory tupleFactory = TupleFactory.getInstance();
  private static BagFactory bagFactory = BagFactory.getInstance();
  private static final GeometryFactory geomFactory = new GeometryFactory();
  
  /**
     Computes the quadkeys for the next level of resolution after @quadKey@.
     Eg. given "1023", returns ["10230","10231","10232","10233"]

     * @param quadKey
     *
     * @return List of 4 quadkey children for @quadKey@
     
     */
  public static ArrayList<String> childrenFor(String quadKey) {
    int res = quadKey.length();

    ArrayList<String> children = new ArrayList<String>();
    StringBuilder child = new StringBuilder(quadKey);
    for (int i = 0; i < 4; i++) {
      child.append(i);
      children.add(child.toString());
      child.deleteCharAt(res);
    }
    return children;
  }

  /**
     Converts an array of Coordinates to tile Coordinates at a given zoom level.

     * @param coordinates
     *            Array of Coordinates
     * @param zoomLevel
     *        Resolution for the tile Coordinates, from 1 (lowest detail) to 23 (highest detail)
     *
     * @return Array of tile Coordinates
     
     */
  public static Coordinate [] latLngToTileCoords(Coordinate [] coordinates,int zoomLevel) {
    Coordinate [] tileCoordinates = new Coordinate[coordinates.length-1];
    for(int i = 0; i < coordinates.length-1; i++) {
      Coordinate latLngPoint = coordinates[i];
      int[] pixelXY = QuadKeyUtils.geoPointToPixelXY(latLngPoint.x, latLngPoint.y, zoomLevel);
      int[] tileXY = QuadKeyUtils.pixelXYToTileXY(pixelXY[0], pixelXY[1]);
      Coordinate coord = new Coordinate(tileXY[0],tileXY[1]);
      tileCoordinates[i] = coord;
    }
    return tileCoordinates;
  }    

  /**
     Computes the bounding box of a quadKey.

     * @param quadKey
     *
     * @return Polygon tile corresponding to @quadKey@. 
     
     */
  public static Polygon quadKeyToBox(String quadKey) {

    int[] tileXY = quadKeyToTileXY(quadKey);
    int[] pixelXYMin = tileXYToPixelXY(tileXY[0], tileXY[1]);
    int[] pixelXYMax = {pixelXYMin[0] + 256, pixelXYMin[1] + 256};

    //convert to latitude and longitude coordinates
    int levelOfDetail = quadKey.length();
    double mapsize = mapSize(levelOfDetail);
        
    double xmin = (clip(pixelXYMin[0], 0, mapsize - 1) / mapsize) - 0.5;
    double ymin = 0.5 - (clip(pixelXYMin[1], 0, mapsize - 1) / mapsize);

    double north = 90 - 360 * Math.atan(Math.exp(-ymin * 2 * Math.PI)) / Math.PI;
    double west = 360 * xmin;

    double xmax = (clip(pixelXYMax[0], 0, mapsize - 1) / mapsize) - 0.5;
    double ymax = 0.5 - (clip(pixelXYMax[1], 0, mapsize - 1) / mapsize);

    double south = 90 - 360 * Math.atan(Math.exp(-ymax * 2 * Math.PI)) / Math.PI;
    double east = 360 * xmax;

    Coordinate nw = new Coordinate(west, north);
    Coordinate ne = new Coordinate(east, north);
    Coordinate sw = new Coordinate(west, south);
    Coordinate se = new Coordinate(east, south);
    
    Coordinate[] bboxCoordinates = {nw, ne, se, sw, nw};    
    LinearRing bboxRing = geomFactory.createLinearRing(bboxCoordinates);
    Polygon bbox_poly = geomFactory.createPolygon(bboxRing, null);
    
    return bbox_poly;
  } 

  /**
   * Determines the map width and height (in pixels) at a specified level of detail.
   * 
   * @param levelOfDetail
   *            Level of detail, from 1 (lowest detail) to 23 (highest detail)
   * @return The map width and height in pixels
   */
  public static int mapSize(final int levelOfDetail) {
    return TILE_SIZE << levelOfDetail;
  }
    
  /**
   * Clips a number to the specified minimum and maximum values.
   * 
   * @param n
   *            The number to clip
   * @param minValue
   *            Minimum allowable value
   * @param maxValue
   *            Maximum allowable value
   * @return The clipped value.
   */
  private static double clip(final double n, final double minValue, final double maxValue) {
    return Math.min(Math.max(n, minValue), maxValue);
  }

  public static String geoPointToQuadKey(double longitude, double latitude, final int levelOfDetail) {
    int[] pixelXY = geoPointToPixelXY(longitude, latitude, levelOfDetail);
    int[] tileXY = pixelXYToTileXY(pixelXY[0], pixelXY[1]);
    return tileXYToQuadKey(tileXY[0], tileXY[1], levelOfDetail);
  }
    
  /**
   * Converts a point from latitude/longitude WGS-84 coordinates (in degrees) into pixel XY
   * coordinates at a specified level of detail.
   * 
   * @param latitude
   *            Latitude of the point, in degrees
   * @param longitude
   *            Longitude of the point, in degrees
   * @param levelOfDetail
   *            Level of detail, from 1 (lowest detail) to 23 (highest detail)
   *
   * @return Output parameter receiving the X and Y coordinates in pixels
   */
  public static int[] geoPointToPixelXY(double longitude, double latitude, final int levelOfDetail) {
    latitude = clip(latitude, MIN_LATITUDE, MAX_LATITUDE);
    longitude = clip(longitude, MIN_LONGITUDE, MAX_LONGITUDE);

    final double x = (longitude + 180) / 360;
    final double sinLatitude = Math.sin(latitude * Math.PI / 180);
    final double y = 0.5 - Math.log((1 + sinLatitude) / (1 - sinLatitude)) / (4 * Math.PI);

    final int mapSize = mapSize(levelOfDetail);
    int[] pixelXY = {(int) clip(x * mapSize + 0.5, 0, mapSize - 1), (int) clip(y * mapSize + 0.5, 0, mapSize - 1)};
    return pixelXY;
  }

  /**
   * Converts pixel XY coordinates into tile XY coordinates of the tile containing the specified
   * pixel.
   * 
   * @param pixelX
   *            Pixel X coordinate
   * @param pixelY
   *            Pixel Y coordinate
   *
   * @return Output parameter receiving the tile X and Y coordinates
   */
  public static int[] pixelXYToTileXY(final int pixelX, final int pixelY) {
    int[] tileXY = {pixelX / TILE_SIZE, pixelY / TILE_SIZE};
    return tileXY;
  }
    
  /**
   * Converts tile XY coordinates into pixel XY coordinates of the upper-left pixel of the
   * specified tile.
   * 
   * @param tileX
   *            Tile X coordinate
   * @param tileY
   *            Tile X coordinate
   *
   * @return Output parameter receiving the pixel X and Y coordinates
   */
  public static int[] tileXYToPixelXY(final int tileX, final int tileY) {
    int[] pixelXY = {tileX * TILE_SIZE, tileY * TILE_SIZE};
    return pixelXY;
  }

  /**
   * Converts tile XY coordinates into a QuadKey at a specified level of detail.
   * 
   * @param tileX
   *            Tile X coordinate
   * @param tileY
   *            Tile Y coordinate
   * @param levelOfDetail
   *            Level of detail, from 1 (lowest detail) to 23 (highest detail)
   * @return A string containing the QuadKey
   */
  public static String tileXYToQuadKey(final int tileX, final int tileY, final int levelOfDetail) {
    final StringBuilder quadKey = new StringBuilder();
    for (int i = levelOfDetail; i > 0; i--) {
      char digit = '0';
      final int mask = 1 << (i - 1);
      if ((tileX & mask) != 0) {
        digit++;
      }
      if ((tileY & mask) != 0) {
        digit++;
        digit++;
      }
      quadKey.append(digit);
    }
    return quadKey.toString();
  }

  /**
   * Converts a QuadKey into tile XY coordinates.
   * 
   * @param quadKey
   *
   * @return Output parameter receiving the tile X and y coordinates
   */
  public static int[] quadKeyToTileXY(final String quadKey) {
    int tileX = 0;
    int tileY = 0;

    final int levelOfDetail = quadKey.length();
    for (int i = levelOfDetail; i > 0; i--) {
      final int mask = 1 << (i - 1);
      switch (quadKey.charAt(levelOfDetail - i)) {
      case '0':
        break;
      case '1':
        tileX |= mask;
        break;
      case '2':
        tileY |= mask;
        break;
      case '3':
        tileX |= mask;
        tileY |= mask;
        break;

      default:
        throw new IllegalArgumentException("Invalid quadkey digit sequence.");
      }
    }
    int[] tileXY = {tileX, tileY};
    return tileXY;
  }

}
