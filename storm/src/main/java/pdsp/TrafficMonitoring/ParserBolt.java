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

import java.util.Map;

public class ParserBolt extends BaseRichBolt {
    private OutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        long processingTimestamp = System.currentTimeMillis();
        try {
            String data = tuple.getStringByField("line");
            System.out.println("Data received: " + data);
            // example line received:
            //{"vehicle_id": "CAR958", "location": {"latitude": 35.41630051305599, "longitude": -97.05498917164667}, "direction": 317, "speed": 52, "timestamp": "2024-06-17T12:30:00Z"}
            // id,dontcare,timestamp,latitude,longitude,speed,direction
            // 4186585,33556,2009-01-06T05:15:07,40.0423,116.61,0,0

            // Split the data into fields
            String[] fields = data.split(",");
            if(fields.length == 7){
                String vehicleId  = fields[0];

                String timestamp = fields[2];
                double latitude    = Double.parseDouble(fields[3]);
                double longitude    = Double.parseDouble(fields[4]);
                double speed     = Double.parseDouble(fields[5]);
                double direction   = Double.parseDouble(fields[6]);
                // Create a TrafficEvent object
                TrafficEvent event = new TrafficEvent(vehicleId, latitude, longitude, direction, speed, timestamp);
                collector.emit(new Values(event), tuple.getLongByField("e2eTimestamp"), processingTimestamp);
            }

            /* json parser
            // Parse the data and extract the required fields
            JSONParser parser = new JSONParser();
            JSONObject json = (JSONObject) parser.parse(data);
            String vehicleId = (String) json.get("vehicle_id");
            JSONObject location = (JSONObject) json.get("location");
            //get datatype of latitude and longitude
            System.out.println("latitude datatype: "+location.get("latitude").getClass());
            double latitude = (double) location.get("latitude");
            System.out.println("latitude fine");
            double longitude = (double) location.get("longitude");
            System.out.println("longitude fine");
            double direction = (double) json.get("direction");
            System.out.println("direction fine");
            double speed = (double) json.get("speed");
            System.out.println("speed fine");
            String timestamp = (String) json.get("timestamp");
            */





        }
        catch (Exception e) {
            System.out.println("Error in TMParserBolt: " + e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("trafficEvent", "e2eTimestamp", "processingTimestamp"));
    }
}
