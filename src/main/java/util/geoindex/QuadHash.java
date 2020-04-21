package util.geoindex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class QuadHash {
    public static Tile getTile(double lat, double lon, int zoom) {
        int xtile = (int) Math.floor((lon + 180)/360*Math.pow(2, zoom));
        int ytile = (int) Math.floor( (1 - Math.log(Math.tan(Math.toRadians(lat)) + 1 /
                Math.cos(Math.toRadians(lat))) / Math.PI) / 2 * Math.pow(2, zoom) );
        return new Tile(xtile, ytile, zoom);
    }

    public static String getQuadKey(Tile tile) {
        return getQuadKey(tile.getX(), tile.getY(), tile.getZoom());
    }

    public static String getQuadKey(int x, int y, int zoom){
        StringBuilder quadKey = new StringBuilder();
        for (int i = zoom; i > 0; i--) {
            char digit = '0';
            int mask = 1 << (i - 1);
            if ((x & mask) != 0) {
                digit++;
            }
            if ((y & mask) != 0) {
                digit++;
                digit++;
            }
            quadKey.append(digit);
        }
        return quadKey.toString();
    }

    public static String getSlippyKey(String quadKey) {
        int x = 0;
        int y = 0;
        int z = 0;
        String[] quadChars = quadKey.split("");
        for (String c : quadChars){
            x *= 2;
            y *= 2;
            z++;
            if( c.equals("1") || c.equals("3") ){
                x++;
            }
            if( c.equals("2") || c.equals("3") ){
                y++;
            }
        }
        return z + "/" + x + "/" + y;
    }

    public static List<String> coverBoundingBox(Tile minTile, Tile maxTile) {
        assert minTile.getZoom() == maxTile.getZoom();
        List<String> quadKeys = new ArrayList<>();
        for (int x = minTile.getX(); x <= maxTile.getX(); x++){
            for (int y = maxTile.getY(); y <= minTile.getY(); y++) {
                quadKeys.add(getQuadKey(new Tile(x, y, maxTile.getZoom())));
            }
        }
        Collections.sort(quadKeys);
        return quadKeys;
    }

    public static List<String> coverBoundingBox(double topLeftLat, double topLeftLon, double bottomRightLat, double bottomRightLon, int zoom) {
        Tile minTile = getTile(bottomRightLat, topLeftLon, zoom);
        Tile maxTile = getTile(topLeftLat, bottomRightLon, zoom);
//        System.out.println("minTile" + minTile);
//        System.out.println("maxTile" + maxTile);
        return coverBoundingBox(minTile, maxTile);
    }

}