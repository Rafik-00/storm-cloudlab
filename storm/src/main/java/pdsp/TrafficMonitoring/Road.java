package pdsp.TrafficMonitoring;

import java.io.Serializable;

public class Road implements Serializable {
    private String roadId;
    private double latitude1;
    private double longitude1;
    private double latitude2;
    private double longitude2;
    private double speed;
    private int count;
    private double averageSpeed;

    public Road() {
    }
    public Road(String roadId, double latitude1, double longitude1, double latitude2, double longitude2) {
        this.roadId = roadId;
        this.latitude1 = latitude1;
        this.longitude1 = longitude1;
        this.latitude2 = latitude2;
        this.longitude2 = longitude2;
        this.speed = 0;
        this.count = 0;
        this.averageSpeed = 0;
    }

    public String getRoadId() {
        return roadId;
    }

    public void setRoadId(String roadId) {
        this.roadId = roadId;
    }

    public double getLatitude1() {
        return latitude1;
    }

    public void setLatitude1(double latitude1) {
        this.latitude1 = latitude1;
    }

    public double getLongitude1() {
        return longitude1;
    }

    public void setLongitude1(double longitude1) {
        this.longitude1 = longitude1;
    }
    public double getLatitude2() {
        return latitude2;
    }
    public void setLatitude2(double latitude2) {
        this.latitude2 = latitude2;
    }
    public double getLongitude2() {
        return longitude2;
    }
    public void setLongitude2(double longitude2) {
        this.longitude2 = longitude2;
    }
    public double getMinLatitude() {
        return Math.min(latitude1, latitude2);
    }
    public double getMaxLatitude() {
        return Math.max(latitude1, latitude2);
    }
    public double getMinLongitude() {
        return Math.min(longitude1, longitude2);
    }
    public double getMaxLongitude() {
        return Math.max(longitude1, longitude2);
    }


    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public double getAverageSpeed() {
        return averageSpeed;
    }

    public void setAverageSpeed(double averageSpeed) {
        this.averageSpeed = averageSpeed;
    }

    public void addSpeed(double speed) {
        this.speed += speed;
        this.count++;
        this.averageSpeed = this.speed / this.count;
    }

}
