package pdsp.wordCount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WordCountBolt extends BaseWindowedBolt {
    private Map<String, Integer> wordCounts = new HashMap<>();
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }
    @Override
    public void execute(TupleWindow inputWindow) {
        long e2eTimestamp = Long.MAX_VALUE;
        long processingTimestamp = 0;

        for (Tuple tuple : inputWindow.get()) {
            String word = (String) tuple.getValueByField("word");

            long currentE2eTimestamp = tuple.getLongByField("e2eTimestamp");
            long currentProcessingTimestamp = tuple.getLongByField("processingTimestamp");

            if (currentE2eTimestamp < e2eTimestamp) {
                e2eTimestamp = currentE2eTimestamp;
            }
            if (currentProcessingTimestamp > processingTimestamp) {
                processingTimestamp = currentProcessingTimestamp;
            }

            if (wordCounts.containsKey(word)) {
                wordCounts.put(word, wordCounts.get(word) + 1);
                collector.emit(new Values(word, wordCounts.get(word), tuple.getLongByField("e2eTimestamp"), tuple.getLongByField("processingTimestamp")));
            } else {
                wordCounts.put(word, 1);
                collector.emit(new Values(word, 1, tuple.getLongByField("e2eTimestamp"), tuple.getLongByField("processingTimestamp")));
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word", "count", "e2eTimestamp", "processingTimestamp"));
    }
}
