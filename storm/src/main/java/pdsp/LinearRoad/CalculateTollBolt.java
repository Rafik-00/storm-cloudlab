package pdsp.LinearRoad;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class CalculateTollBolt extends CalculatorBolt {
    Map<String, Integer> tollPerSegment;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        super.prepare(stormConf, context, collector);

        this.tollPerSegment = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {


        String segmentID = input.getStringByField("segmentID");
        int numVehicles = input.getIntegerByField("numVehicles");
        double averageSpeed = input.getDoubleByField("averageSpeedPerSegment");

        int toll = calculateToll(averageSpeed, numVehicles);

        System.out.println("Segment " + segmentID + " Toll " + toll);

        collector.emit(new Values(segmentID, toll, input.getLongByField("e2eTimestamp"), input.getLongByField("processingTimestamp")));


    }


    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("segmentID", "toll", "e2eTimestamp", "processingTimestamp"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return Map.of();
    }
}
