package pdsp.googleCloudMonitoring;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.List;
import java.util.Map;

public class CPUPerCategoryBolt extends BaseWindowedBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        List<Tuple> tuples = tupleWindow.get();
        long minTimestamp = Long.MAX_VALUE;
        int category = -1;
        float sum = 0;

        long e2eTimestamp = Long.MAX_VALUE, processingTimestamp = 0;

        for (Tuple tuple : tuples) {
            TaskEvent event = (TaskEvent) tuple.getValueByField("taskEvent");
            if (minTimestamp >= event.getTimestamp()) {
                minTimestamp = event.getTimestamp();
            }
            category = event.getCategory();
            sum += event.getCpu();

            e2eTimestamp = Math.min(e2eTimestamp, tuple.getLongByField("e2eTimestamp"));
            processingTimestamp = Math.max(processingTimestamp, tuple.getLongByField("processingTimestamp"));
        }

        CPUPerCategory result = new CPUPerCategory(minTimestamp, category, sum);

        collector.emit(new Values(result, e2eTimestamp, processingTimestamp));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("cpuPerCategory", "e2eTimestamp", "processingTimestamp"));
    }
}
