package indexingTopology.util.shape;

import java.io.Serializable;

/**
 * Created by billlin on 2018/3/2
 */
public class TrackItem implements Shape, Serializable{

    private double longitude;
    private double latitude;
    private double radius;

    public TrackItem(double longitude, double latitude, double radius) {
        this.longitude = longitude;
        this.latitude = latitude;
        this.radius = radius;
    }

    public double getLongitude() {
        return longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getRadius() {
        return radius;
    }

    @Override
    public boolean checkIn(Point point) {
        double pointX = point.x, pointY = point.y;
        double len = Math.sqrt(Math.pow(pointX - longitude, 2) + Math.pow(pointY - latitude, 2));
        if(radius >= len) {
            return true;
        }
        return false;
    }

    @Override
    public Rectangle getExternalRectangle() {
        Point leftTop = new Point(longitude - radius, latitude + radius);
        Point rightBottom = new Point(longitude + radius, latitude - radius);
        return new Rectangle(leftTop, rightBottom);
    }
}
