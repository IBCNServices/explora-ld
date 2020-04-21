package util.geoindex;

public class Tile {
    private int x;
    private int y;
    private int zoom;

    public Tile() {
    }

    public Tile(int x, int y, int zoom) {
        this.x = x;
        this.y = y;
        this.zoom = zoom;
    }

    public int getX() {
        return x;
    }

    public void setX(int x) {
        this.x = x;
    }

    public int getY() {
        return y;
    }

    public void setY(int y) {
        this.y = y;
    }

    public int getZoom() {
        return zoom;
    }

    public void setZoom(int zoom) {
        this.zoom = zoom;
    }

    @Override
    public String toString() {
        return zoom + "/" + x + "/" + y;
    }
}
