package pdsp.spikedetection;

import java.io.Serializable;

public class SensorMeasurementModel implements Serializable {
    private static final long serialVersionUID = 1L;

    private long timestamp;
    private int sensorId;
    private float temperature;
    private float humidity;
    private float light;
    private float voltage;
    public SensorMeasurementModel() {
    }
    public SensorMeasurementModel(long timestamp, int sensorId, float temperature, float humidity, float light, float voltage) {
        this.timestamp = timestamp;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.humidity = humidity;
        this.light = light;
        this.voltage = voltage;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getSensorId() {
        return sensorId;
    }

    public float getTemperature() {
        return temperature;
    }

    public float getHumidity() {
        return humidity;
    }

    public float getLight() {
        return light;
    }

    public float getVoltage() {
        return voltage;
    }

    @Override
    public String toString() {
        return "SensorMeasurementModel{" +
                "timestamp=" + timestamp +
                ", sensorId=" + sensorId +
                ", temperature=" + temperature +
                ", humidity=" + humidity +
                ", light=" + light +
                ", voltage=" + voltage +
                '}';
    }
}
