package pdsp.adAnalytics;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

public class RollingCTRCalculatorBolt extends BaseWindowedBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        System.out.println("Preparing RollingCTRBolt");
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        Map<String, List<Float>> rollingCTRValues = new HashMap<>();
        List<Long> allE2eTimestamps = new ArrayList<>();
        List<Long> allProcessingTimestamps = new ArrayList<>();

        long e2eTimestamp = Long.MAX_VALUE, processingTimestamp = 0;

        // Iterate over tuples in the window
        for (Tuple tuple : inputWindow.get()) {
            String type = tuple.getStringByField("type");
            String queryId = tuple.getStringByField("queryId");
            String adId = tuple.getStringByField("adId");
            float ctrValue = tuple.getFloatByField("ctrValue");

            // Build a key to identify the ad event
            String key = queryId + "-" + adId;

            // Add the CTR value to the rolling CTR values list
            rollingCTRValues.computeIfAbsent(key, k -> new ArrayList<>()).add(ctrValue);

            // Add timestamps
            long currentE2eTimestamp = tuple.getLongByField("e2eTimestamp");
            long currentProcessingTimestamp = tuple.getLongByField("processingTimestamp");

            allE2eTimestamps.add(currentE2eTimestamp);
            allProcessingTimestamps.add(currentProcessingTimestamp);

            // Update min e2eTimestamp and max processingTimestamp
            if (currentE2eTimestamp < e2eTimestamp) {
                e2eTimestamp = currentE2eTimestamp;
            }
            if (currentProcessingTimestamp > processingTimestamp) {
                processingTimestamp = currentProcessingTimestamp;
            }
        }

        // Calculate and emit rolling CTR
        for (Map.Entry<String, List<Float>> entry : rollingCTRValues.entrySet()) {
            String key = entry.getKey();
            List<Float> ctrValues = entry.getValue();

            // Calculate the rolling CTR
            float rollingCTR = calculateRollingCTR(ctrValues);
            String[] parts = key.split("-");
            String queryId = parts[0];
            String adId = parts[1];

            System.out.println("Rolling CTR for " + key + ": " + rollingCTR);

            // Emit the rolling CTR along with the ad event details and timestamps
            collector.emit(new Values(queryId, adId, rollingCTR, e2eTimestamp, processingTimestamp));
        }
    }

    private float calculateRollingCTR(List<Float> ctrValues) {
        // Calculate the average of the CTR values in the list
        float sum = 0;
        for (float value : ctrValues) {
            sum += value;
        }
        return sum / ctrValues.size();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("queryId", "adId", "rollingCTR", "e2eTimestamp", "processingTimestamp"));
    }

    @Override
    public void cleanup() {
        // Cleanup logic, if needed
    }
}
