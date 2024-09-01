package pdsp.logsAnalytics;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
/**
 * Created by Rafik Youssef on 05.06.2024.
 * This class is responsible for counting the number of visits per minute.
 */
public class volumeCounterBolt extends BaseRichBolt {
    private Map<String, Integer> volumeCounts;
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.volumeCounts = new HashMap<>();
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String logTime = tuple.getStringByField("logTime");
        System.out.println("LogTime received: " + logTime);
        String minute = logTime.substring(0, 17);
        System.out.println("Minute: " + minute);
        if (volumeCounts.containsKey(minute)) {
            volumeCounts.put(minute, volumeCounts.get(minute) + 1);
        } else {
            volumeCounts.put(minute, 1);
        }
        //print the volumeCounts map
        for (Map.Entry<String, Integer> entry : volumeCounts.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
        collector.emit(new Values(volumeCounts, tuple.getLongByField("e2eTimestamp"), tuple.getLongByField("processingTimestamp")));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("volumeCounts", "e2eTimestamp", "processingTimestamp"));
    }
}
