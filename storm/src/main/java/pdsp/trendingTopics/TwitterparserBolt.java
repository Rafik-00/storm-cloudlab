package pdsp.trendingTopics;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import pdsp.utils.KafkaUtils;

import java.util.Map;


public class TwitterparserBolt implements IRichBolt {

    OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

    }

    @Override
    public void execute(Tuple tuple) {

        long processingTimestamp = System.currentTimeMillis();
        long e2eTimestamp;
        String line;



        try {
            e2eTimestamp = tuple.getLongByField("e2eTimestamp");
            line = tuple.getString(0);
        } catch (IllegalArgumentException e) {
            String tupleStr = (String) tuple.getValue(4);
            Object[] arr = KafkaUtils.parseValue(tupleStr);
            line = (String) arr[0];
            e2eTimestamp = arr[1] != null ? (long) arr[1] : processingTimestamp;
        }





       // line = tuple.getStringByField("line");

        String[] values = line.split(",");
        String id = values[1];
        String timestamp = values[2];
        String text = values[5];

        System.out.println(" Tweet: id: " + id + " timestamp: " + timestamp + " text: " + text);
        collector.emit(new Values(id, timestamp, line, e2eTimestamp, processingTimestamp));
        collector.ack(tuple);


    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("id", "timestamp", "line","e2eTimestamp", "processingTimestamp"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return Map.of();
    }
}
