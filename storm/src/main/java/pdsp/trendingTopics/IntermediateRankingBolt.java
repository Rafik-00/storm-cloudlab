package pdsp.trendingTopics;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

public class IntermediateRankingBolt extends BaseRichBolt {

    OutputCollector collector;
    private Map<String, Integer> topicCounts;
    private PriorityQueue<Map.Entry<String, Integer>> rankerQueue;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        this.collector = collector;
        this.topicCounts = new HashMap<>();
        this.rankerQueue = new PriorityQueue<>((a, b) -> b.getValue() - a.getValue());
    }

    @Override
    public void execute(Tuple input) {


        String topic = input.getStringByField("topic");
        Integer topicCount = input.getIntegerByField("topicCount");


        this.topicCounts.put(topic, topicCount);

        //build ranking
        rankerQueue.clear();
        rankerQueue.addAll(topicCounts.entrySet());

        // Emit the rankings
        int rank = 1;
        for (Map.Entry<String, Integer> entry : rankerQueue) {
            collector.emit(new Values(rank, entry.getKey(), entry.getValue(),input.getLongByField("e2eTimestamp"), input.getLongByField("processingTimestamp")));
            rank++;


        }
        collector.ack(input);


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("rank", "topic", "topicCount", "e2eTimestamp", "processingTimestamp"));

    }
}