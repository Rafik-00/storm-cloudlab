package pdsp.trendingTopics;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.Map;


/**
 * counts the occurences of topics
 */

public class RollingCounterSlidingWindowBolt extends BaseWindowedBolt {
    private OutputCollector collector;
    Map<String, Integer> topicCount;


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.topicCount = new HashMap<>();
    }

    @Override
    public void execute(TupleWindow inputWindow) {

        long e2eTimestamp = Long.MAX_VALUE, processingTimestamp = 0;
        for (Tuple tuple : inputWindow.get()) {
            long tupleE2eTimestamp = tuple.getLongByField("e2eTimestamp");
            long tupleProcessingTimestamp = tuple.getLongByField("processingTimestamp");
            e2eTimestamp = Math.min(e2eTimestamp, tupleE2eTimestamp);
            processingTimestamp = Math.max(processingTimestamp, tupleProcessingTimestamp);
        }

        for (Tuple tuple : inputWindow.get()) {

            String topic = tuple.getStringByField("topic");

            topicCount.put(topic, topicCount.getOrDefault(topic, 0) + 1);


            collector.emit(new Values(topic, topicCount.get(topic), e2eTimestamp, processingTimestamp));
            collector.ack(tuple);

        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("topic", "topicCount", "e2eTimestamp", "processingTimestamp"));
    }
}

