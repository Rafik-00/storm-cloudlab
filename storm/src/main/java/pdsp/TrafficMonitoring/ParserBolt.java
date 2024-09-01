package pdsp.TrafficMonitoring;

import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import pdsp.utils.KafkaUtils;

import java.util.Map;

public class ParserBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        long processingTimestamp = System.currentTimeMillis();
        long e2eTimestamp;
        String data;
        try {
            data = tuple.getStringByField("line");
            e2eTimestamp = tuple.getLongByField("e2eTimestamp");
//            System.out.println("Data received: " + data);

            // Split the data into fields
            String[] fields = data.split(",");
            if (fields.length == 7) {
                String vehicleId = fields[0];

                String timestamp = fields[2];
                double latitude = Double.parseDouble(fields[3]);
                double longitude = Double.parseDouble(fields[4]);
                double speed = Double.parseDouble(fields[5]);
                double direction = Double.parseDouble(fields[6]);
                // Create a TrafficEvent object
                TrafficEvent event = new TrafficEvent(vehicleId, latitude, longitude, direction, speed, timestamp);
                outputCollector.emit(new Values(event, e2eTimestamp, processingTimestamp));
            }
        } catch (Exception e) {
            String tupleStr = (String) tuple.getValue(4);
            Object[] arr = KafkaUtils.parseValue(tupleStr);
            data = (String) arr[0];
            e2eTimestamp = arr[1] != null ? (long) arr[1] : processingTimestamp;
//            System.out.println("Data received: " + data);

            // Split the data into fields
            String[] fields = data.split(",");
            if (fields.length == 7) {
                String vehicleId = fields[0];

                String timestamp = fields[2];
                double latitude = Double.parseDouble(fields[3]);
                double longitude = Double.parseDouble(fields[4]);
                double speed = Double.parseDouble(fields[5]);
                double direction = Double.parseDouble(fields[6]);
                // Create a TrafficEvent object
                TrafficEvent event = new TrafficEvent(vehicleId, latitude, longitude, direction, speed, timestamp);
                outputCollector.emit(new Values(event, e2eTimestamp, processingTimestamp));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("trafficEvent", "e2eTimestamp", "processingTimestamp"));
    }
}
