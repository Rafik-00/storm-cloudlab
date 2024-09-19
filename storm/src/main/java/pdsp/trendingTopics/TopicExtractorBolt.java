package pdsp.trendingTopics;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


import java.util.Map;


/**
 * Splits the Tweet into words and groups
 */

public class TopicExtractorBolt implements IRichBolt {

    private OutputCollector collector;


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {


        String line = input.getStringByField("line");
        String topic = null;


        for (String word : line.split("\\s+")) {
            if (word.startsWith("@") || word.startsWith("#")) {
                topic = word.substring(1);

                collector.emit(new Values(topic, input.getLongByField("e2eTimestamp"), input.getLongByField("processingTimestamp")));
                collector.ack(input);

            }
        }

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("topic", "e2eTimestamp", "processingTimestamp"));


    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return Map.of();
    }
}


