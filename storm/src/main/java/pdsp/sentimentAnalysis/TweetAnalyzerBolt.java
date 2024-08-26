package pdsp.sentimentAnalysis;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class TweetAnalyzerBolt extends BaseRichBolt {
    OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        SentimentClassifier classifier = SentimentClassifierFactory.create(SentimentClassifierFactory.BASIC);
        SentimentResult sentimentResult = classifier.classify(tuple.getStringByField("text"));
        collector.emit(new Values(
                tuple.getStringByField("id"),
                tuple.getStringByField("timestamp"),
                tuple.getStringByField("text"),
                sentimentResult.getSentiment(),
                sentimentResult.getScore(),
                tuple.getLongByField("e2eTimestamp"),
                tuple.getLongByField("processingTimestamp")
        ));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id", "timestamp", "text", "sentiment", "score", "e2eTimestamp", "processingTimestamp"));
    }
}
