package pdsp.spikedetection;

import java.io.Serializable;

public class AverageValueModel implements Serializable {
    public int sensorId;
    public float currentValue;
    public float averageValue;

    public AverageValueModel() {
    }

    public AverageValueModel(int sensorId, float currentValue, float averageValue) {
        this.sensorId = sensorId;
        this.currentValue = currentValue;
        this.averageValue = averageValue;
    }

    public int getSensorId() {
        return sensorId;
    }

    public void setSensorId(int sensorId) {
        this.sensorId = sensorId;
    }

    public float getCurrentValue() {
        return currentValue;
    }

    public void setCurrentValue(float currentValue) {
        this.currentValue = currentValue;
    }

    public float getAverageValue() {
        return averageValue;
    }

    public void setAverageValue(float averageValue) {
        this.averageValue = averageValue;
    }

    @Override
    public String toString() {
        return "AverageValue{" +
                "sensorId=" + sensorId +
                ", currentValue=" + currentValue +
                ", averageValue=" + averageValue +
                '}';
    }
}

