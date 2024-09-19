package pdsp.trendingTopics;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;


public class FinalRankingBolt extends BaseRichBolt {

    int topicPopularityThreshold;

    public FinalRankingBolt(int topicPopularityThreshold) {

        this.topicPopularityThreshold = topicPopularityThreshold;

    }

    OutputCollector collector;


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        this.collector = collector;

    }

    @Override
    public void execute(Tuple input) {



        Integer rank = input.getIntegerByField("rank");
        String topic = input.getStringByField("topic");
        Integer topicCount = input.getIntegerByField("topicCount");


        if (topicCount > topicPopularityThreshold) {


            collector.emit(new Values(rank, topic, topicCount, input.getLongByField("e2eTimestamp"), input.getLongByField("processingTimestamp")));
            System.out.println("FinalRank " + rank + " Topic " + topic + " count " + topicCount);


        }
        collector.ack(input);

    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("finalRank", "topic", "topicCount","e2eTimestamp", "processingTimestamp"));

    }
}
