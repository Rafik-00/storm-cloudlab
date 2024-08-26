package pdsp.TrafficMonitoring;

import java.io.Serializable;

public class TrafficEvent implements Serializable {
    private  String vehicleId;
    private double latitude;
    private double longitude;
    private double direction;
    private double speed;
    private String timestamp;

    //no-arg constructor
    public TrafficEvent() {
    }



    public String getVehicleId() {
        return vehicleId;
    }

    public void setVehicleId(String vehicleId) {
        this.vehicleId = vehicleId;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getDirection() {
        return direction;
    }

    public void setDirection(double direction) {
        this.direction = direction;
    }

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public TrafficEvent(String vehicleId, double latitude, double longitude, double direction, double speed, String timestamp) {
        this.vehicleId = vehicleId;
        this.latitude = latitude;
        this.longitude = longitude;
        this.direction = direction;
        this.speed = speed;
        this.timestamp = timestamp;
    }

}
