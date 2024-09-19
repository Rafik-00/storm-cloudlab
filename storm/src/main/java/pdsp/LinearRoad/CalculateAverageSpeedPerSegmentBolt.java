package pdsp.LinearRoad;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class CalculateAverageSpeedPerSegmentBolt extends BaseRichBolt {

    OutputCollector collector;
    // sum of speed per Segment
    private Map<String, Integer> speedSum;
    //num of vehicles per segment
    private Map<String, Integer> numVehiclesPerSegment;
    // map to store previous segment per vehicle
    private Map<Integer, String> previousSegmentOfVehicle;
    // map to store previous speed per vehicle
    private Map<Integer, Integer> previousSpeedOfVehicle;


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        this.collector = collector;
        this.speedSum = new HashMap<>();
        this.numVehiclesPerSegment = new HashMap<>();
        this.previousSegmentOfVehicle = new HashMap<>();
        this.previousSpeedOfVehicle = new HashMap<>();

    }

    @Override
    public void execute(Tuple input) {

        int type = input.getIntegerByField("type");
        int xway = input.getIntegerByField("xway");
        int segment = input.getIntegerByField("segment");
        int vehicleId = input.getIntegerByField("vehicleID");
        String segmentID = "xway " + xway + " segment " + segment;
        int speed = input.getIntegerByField("speed");

        // input is a position report: maps need to be updated
        if (type == 0) {

            //first vehicle of segment arrives
            if (!numVehiclesPerSegment.containsKey(segmentID)) {
                numVehiclesPerSegment.put(segmentID, 1);
            } else {
                //add vehicle in segment
                numVehiclesPerSegment.put(segmentID, numVehiclesPerSegment.get(segmentID) + 1);
            }

            // Update number of vehicles in last segment

            //vehicle arrives for the first time
            if (!previousSegmentOfVehicle.containsKey(vehicleId) || !speedSum.containsKey(segmentID)) {

                previousSegmentOfVehicle.put(vehicleId, segmentID);
                previousSpeedOfVehicle.put(vehicleId, speed);
                speedSum.put(segmentID, speedSum.getOrDefault(segmentID, 0) + speed);

            }
            //vehicle was already on the roads
            else {

                // decrease numberOfVehicles in previous Segment and put currentSegment as last Segment
                String previousSegment = previousSegmentOfVehicle.get(vehicleId);
                numVehiclesPerSegment.put(previousSegment, numVehiclesPerSegment.get(segmentID) - 1);

                // decrease speedSum of previous Segment


                int previousSpeed = previousSpeedOfVehicle.get(vehicleId);
                speedSum.put(segmentID, speedSum.get(segmentID) - previousSpeed);


            }
        }


        double averageSpeedPerSegment = (double) speedSum.get(segmentID) / numVehiclesPerSegment.get(segmentID);

        collector.emit(new Values(segmentID, averageSpeedPerSegment, numVehiclesPerSegment.get(segmentID), input.getLongByField("e2eTimestamp"), input.getLongByField("processingTimestamp")));

        System.out.println("ID " + segmentID + " averageSpeed " + averageSpeedPerSegment);


        collector.ack(input);


    }


    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("segmentID", "averageSpeedPerSegment", "numVehicles", "e2eTimestamp", "processingTimestamp"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return Map.of();
    }
}
