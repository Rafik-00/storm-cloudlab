package pdsp.clickAnalytics;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import pdsp.utils.KafkaUtils;

import java.util.Map;

public class ClickLogParserBolt extends BaseRichBolt {
    private OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        long processingTimestamp = System.currentTimeMillis();
        long e2eTimestamp;
        String s;

        try {
            e2eTimestamp = tuple.getLongByField("e2eTimestamp");
            s = tuple.getString(0);
        } catch (Exception e) {
            try {
                String tupleStr = (String) tuple.getValue(4);
                Object[] tupleValues = KafkaUtils.parseValue(tupleStr);
                e2eTimestamp = (long) tupleValues[1];
                s = (String) tupleValues[0];
            } catch (Exception e1) {
                e1.printStackTrace();
                return;
            }
        }
        JsonObject obj = new JsonParser().parse(s).getAsJsonObject();
        String url = obj.get("url").getAsString();
        String ip = obj.get("ip").getAsString();
        String clientKey = obj.get("clientKey").getAsString();

        outputCollector.emit(new Values(
                clientKey,
                new ClickLog(url, ip, clientKey),
                e2eTimestamp,
                processingTimestamp
        ));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("clientKey", "clickLog", "e2eTimestamp", "processingTimestamp"));
    }
}
