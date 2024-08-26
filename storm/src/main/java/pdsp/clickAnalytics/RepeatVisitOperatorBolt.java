package pdsp.clickAnalytics;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.Map;

public class RepeatVisitOperatorBolt extends BaseWindowedBolt {
    private transient Map<String, Boolean> visitTracker;
    private OutputCollector outputCollector;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        this.outputCollector = collector;
        visitTracker = new HashMap<>();
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        long e2eTimestamp = Long.MAX_VALUE, processingTimestamp = 0;
        for (Tuple tuple : tupleWindow.get()) {
            long tupleE2eTimestamp = tuple.getLongByField("e2eTimestamp");
            long tupleProcessingTimestamp = tuple.getLongByField("processingTimestamp");
            e2eTimestamp = Math.min(e2eTimestamp, tupleE2eTimestamp);
            processingTimestamp = Math.max(processingTimestamp, tupleProcessingTimestamp);
        }

        for (Tuple tuple : tupleWindow.get()) {
            ClickLog clickLog = (ClickLog) tuple.getValue(1);
            String url = clickLog.getUrl();
            String clientKey = tuple.getStringByField("clientKey");

            String mapKey = url + clientKey;
            boolean visited = visitTracker.getOrDefault(mapKey, false);

            if (!visited) {
                visitTracker.put(mapKey, true);
                outputCollector.emit(new Values(url, 1, 1, e2eTimestamp, processingTimestamp));
            } else {
                outputCollector.emit(new Values(url, 1, 0, e2eTimestamp, processingTimestamp));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declare(new Fields("url", "totalVisitCounter", "uniqueVisitCounter", "e2eTimestamp", "processingTimestamp"));
    }
}
