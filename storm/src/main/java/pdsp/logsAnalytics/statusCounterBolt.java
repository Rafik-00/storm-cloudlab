package pdsp.logsAnalytics;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class statusCounterBolt extends BaseRichBolt {
    private Map<String, Integer> statusCounts;
    private OutputCollector collector;



    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.statusCounts = new HashMap<>();
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String statusCode = tuple.getStringByField("statusCode");
        System.out.println("Status code received: " + statusCode);
        if (statusCounts.containsKey(statusCode)) {
            statusCounts.put(statusCode, statusCounts.get(statusCode) + 1);
        } else {
            statusCounts.put(statusCode, 1);
        }
        //TODO: Print the statusCounts map
        for (Map.Entry<String, Integer> entry : statusCounts.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("statusCounts"));
    }
}
