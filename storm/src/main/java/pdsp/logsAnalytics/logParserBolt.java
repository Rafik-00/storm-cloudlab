package pdsp.logsAnalytics;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import pdsp.utils.KafkaUtils;

import java.util.Map;

public class logParserBolt extends BaseRichBolt {
    private OutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        long processingTimestamp = System.currentTimeMillis();
        long e2eTimestamp;
        String log;
        try {
            log = tuple.getStringByField("line");
            e2eTimestamp = tuple.getLongByField("e2eTimestamp");
            System.out.println("Log received: " + log);
            // Parse the log and extract the required fields
            String[] record = log.split(" ");
            String logTime = record[3].substring(1);
            String statusCode = record[8];
            System.out.println("LogTime: " + logTime + ", StatusCode: " + statusCode);
            logEvent event = new logEvent(logTime, statusCode);
            collector.emit(new Values(logTime, statusCode, e2eTimestamp, processingTimestamp));
        } catch (Exception e) {
            String tupleStr = (String) tuple.getValue(4);
            Object[] arr = KafkaUtils.parseValue(tupleStr);
            log = (String) arr[0];
            e2eTimestamp = arr[1] != null ? (long) arr[1] : processingTimestamp;

            // Parse the log and extract the required fields
            String[] record = log.split(" ");
            String logTime = record[3].substring(1);
            String statusCode = record[8];
            System.out.println("LogTime: " + logTime + ", StatusCode: " + statusCode);
            logEvent event = new logEvent(logTime, statusCode);
            collector.emit(new Values(logTime, statusCode, e2eTimestamp, processingTimestamp));
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("logTime", "statusCode", "e2eTimestamp", "processingTimestamp"));
    }
}
