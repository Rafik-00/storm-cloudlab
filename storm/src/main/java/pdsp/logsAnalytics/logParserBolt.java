package pdsp.logsAnalytics;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.Map;

public class logParserBolt extends BaseRichBolt {
    private OutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        /* System.out.println("this is printed:"+value);
        String[] parts = value.split(" ");
        for(String part : parts){
            System.out.println(part);
        }
        String logTime = parts[3].substring(1);
        String statusCode = value.split("\"")[2].trim().split(" ")[0];
        System.out.println(statusCode);
        return new LogEvent(logTime, statusCode);*/

        try {
        String log = tuple.getStringByField("line");
        System.out.println("Log received: " + log);
        // Parse the log and extract the required fields
        String[] record = log.split(" ");
        String logTime = record[3].substring(1);
        String statusCode = record[8];
        System.out.println("LogTime: " + logTime + ", StatusCode: " + statusCode);
        logEvent event = new logEvent(logTime, statusCode);
        collector.emit(new Values(logTime, statusCode));
        }
        catch (Exception e) {
            System.out.println("Error in logParserBolt: " + e);
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("logTime", "statusCode"));
    }
}
