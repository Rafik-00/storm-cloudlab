package pdsp.clickAnalytics;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.Map;

// Todo: maybe use IStatefulWindowedBolt instead of BaseWindowedBolt
public class SumReducerBolt extends BaseWindowedBolt {
    private OutputCollector outputCollector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        String url = (String) tupleWindow.get().get(0).getValue(0);
        int totalVisitors = 0;
        int totalUniqueVisitors = 0;

        long e2eTimestamp = Long.MAX_VALUE, processingTimestamp = 0;

        for (Tuple tuple : tupleWindow.get()) {
            int field1 = (int) tuple.getValue(1);
            int field2 = (int) tuple.getValue(2);
            totalVisitors += field1;
            totalUniqueVisitors += field2;

            e2eTimestamp = Math.min(e2eTimestamp, tuple.getLongByField("e2eTimestamp"));
            processingTimestamp = Math.max(processingTimestamp, tuple.getLongByField("processingTimestamp"));
        }
        outputCollector.emit(new Values(url,totalVisitors,totalUniqueVisitors, e2eTimestamp, processingTimestamp));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("field0","sumField1","sumField2", "e2eTimestamp", "processingTimestamp"));
    }
}
