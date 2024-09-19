package pdsp.LinearRoad;

public class TollNotification {

    int vehicleId;

    String segment;

    int toll;


    public TollNotification(int vehicleId, String segment, int toll) {
        this.vehicleId = vehicleId;
        this.segment = segment;
        this.toll = toll;

    }

    public int getVehicleId() {
        return vehicleId;
    }

    public void setVehicleId(int vehicleId) {
        this.vehicleId = vehicleId;
    }

    public String getSegment() {
        return segment;
    }

    public void setSegment(String segment) {
        this.segment = segment;
    }



    public int getToll() {
        return toll;
    }

    public void setToll(int toll) {
        this.toll = toll;
    }

    public String stringFormatter() {

        return "VehicleId: " + vehicleId + " Segment: "+ segment + " Toll: "+ toll;
    }
}
