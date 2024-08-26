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
        String s = tuple.getString(0);
        JsonObject obj = new JsonParser().parse(s).getAsJsonObject();
        String url = obj.get("url").getAsString();
        String ip = obj.get("ip").getAsString();
        String clientKey = obj.get("clientKey").getAsString();

        outputCollector.emit(new Values(
                clientKey,
                new ClickLog(url, ip, clientKey),
                tuple.getValueByField("e2eTimestamp"),
                processingTimestamp
        ));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("clientKey", "clickLog", "e2eTimestamp", "processingTimestamp"));
    }
}
