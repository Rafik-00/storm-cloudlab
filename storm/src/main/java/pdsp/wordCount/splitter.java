package pdsp.wordCount;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import pdsp.utils.KafkaUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class splitter extends BaseBasicBolt {


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word", "e2eTimestamp", "processingTimestamp"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        long processingTimestamp = System.currentTimeMillis();
        long e2eTimestamp;
        String sentence;
        try {
            e2eTimestamp = tuple.getLongByField("e2eTimestamp");
            sentence = tuple.getStringByField("line");
        } catch (IllegalArgumentException e) {
            String tupleStr = (String) tuple.getValue(4);
            Object[] arr = KafkaUtils.parseValue(tupleStr);
            sentence = (String) arr[0];
            e2eTimestamp = arr[1] != null ? (long) arr[1] : processingTimestamp;
        }
        String[] words = sentence.split(" ");
//        List<String> wordList = new ArrayList<>();
        for (String word : words) {
//            wordList.add(word);
            basicOutputCollector.emit(new Values(word, e2eTimestamp, processingTimestamp));

        }

//        basicOutputCollector.emit(new Values(wordList, e2eTimestamp, processingTimestamp));
    }
}
