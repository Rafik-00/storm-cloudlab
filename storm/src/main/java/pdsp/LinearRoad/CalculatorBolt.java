package pdsp.LinearRoad;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class CalculatorBolt extends BaseRichBolt {

    OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {

    }

    // Helper method to calculate the toll based on the average speed and number of vehicles
     int calculateToll(double averageSpeed, int numVehicles) {
        // By default, the toll amount is 2
        int toll = 2;

        // Check if the average speed and number of vehicles meet the conditions for toll assessment
        if (averageSpeed >= 20) {
            toll = (int) ((averageSpeed * numVehicles) / 3);
        }

        return toll;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
