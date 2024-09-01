package pdsp.wordCount;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WordCountBolt extends BaseBasicBolt {
    private Map<String, Integer> wordCounts = new HashMap<>();
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        List<String> words = (List<String>) tuple.getValueByField("sentence");
        System.out.println("Words received: " + words);
        for (String word : words) {
            if (wordCounts.containsKey(word)) {
                wordCounts.put(word, wordCounts.get(word) + 1);
            } else {
                wordCounts.put(word, 1);
            }
        }
        // emit the word and count
        for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
            basicOutputCollector.emit(new Values(entry.getKey(), entry.getValue(), tuple.getLongByField("e2eTimestamp"), tuple.getLongByField("processingTimestamp")));
        }
        // print the hashmap
        for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word", "count", "e2eTimestamp", "processingTimestamp"));
    }
}
