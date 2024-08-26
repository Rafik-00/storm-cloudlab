package pdsp.spikedetection;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class AverageCalculatorBolt extends BaseWindowedBolt {
    private Map<Integer, Queue<Float>> sensorValues;
    private Map<Integer, Float> sumMap;
    private OutputCollector collector;

    public AverageCalculatorBolt() {
        this.sensorValues = new HashMap<>();
        this.sumMap = new HashMap<>();
    }

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        // Variables to hold min and max timestamps
        long e2eTimestamp = Long.MAX_VALUE;
        long processingTimestamp = 0;

        for (Tuple tuple : tupleWindow.get()) {
            SensorMeasurementModel measurement = (SensorMeasurementModel) tuple.getValueByField("measurement");
            int sensorId = measurement.getSensorId();
            float voltage = measurement.getVoltage();

            // Get the timestamps from the tuple
            long currentE2eTimestamp = tuple.getLongByField("e2eTimestamp");
            long currentProcessingTimestamp = tuple.getLongByField("processingTimestamp");

            // Update the min and max timestamps
            if (currentE2eTimestamp < e2eTimestamp) {
                e2eTimestamp = currentE2eTimestamp;
            }
            if (currentProcessingTimestamp > processingTimestamp) {
                processingTimestamp = currentProcessingTimestamp;
            }

            Queue<Float> values = sensorValues.computeIfAbsent(sensorId, k -> new LinkedList<>());
            values.offer(voltage);
            if (values.size() > tupleWindow.get().size()) {
                float removed = values.poll();
                sumMap.put(sensorId, sumMap.get(sensorId) - removed);
            }
            sumMap.put(sensorId, sumMap.getOrDefault(sensorId, 0.0f) + voltage);
            float sum = sumMap.get(sensorId);
            float average = sum / values.size();

            AverageValueModel result = new AverageValueModel(sensorId, voltage, average);
            collector.emit(new Values(sensorId, result, e2eTimestamp, processingTimestamp));
            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sensorId", "averageValue", "e2eTimestamp", "processingTimestamp"));
    }

}
