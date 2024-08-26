package pdsp.adAnalytics;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;




public class AdEventParserBolt implements IRichBolt{
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        System.out.println("Preparing ParserBolt");
    }

    @Override
    public void execute(Tuple tuple) {
        long processingTimestamp = System.currentTimeMillis();
        String line = tuple.getString(0);
        String[] record = line.split("\\s+");;
        String type = record[0];
        int clicks = Integer.parseInt(record[1]);
        int views = Integer.parseInt(record[2]);
        String displayUrl = record[3];
        String adId = record[4];
        String queryId = record[5];
        int depth = Integer.parseInt(record[6]);
        int position = Integer.parseInt(record[7]);
        String advertiserId = record[8];
        String keywordId = record[9];
        String titleId = record[10];
        String descriptionId = record[11];
        String userId = record[12];

        AdEvent event = new AdEvent(type, clicks, views, displayUrl, adId, queryId, depth, position, advertiserId, keywordId, titleId, descriptionId, userId, 1);
        collector.emit(new Values(type, adId, queryId, event, tuple.getValueByField("e2eTimestamp"), processingTimestamp));
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("type", "adId", "queryId", "adEvent","e2eTimestamp", "processingTimestamp"));
    }
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void cleanup() {
    }
}
