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

public class CPUPerJobBolt extends BaseWindowedBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        List<Tuple> tuples = tupleWindow.get();
        long minTaskTimestamp = Long.MAX_VALUE;
        long jobId = -1L;
        float sum = 0;
        int count = 0;

        long e2eTimestamp = Long.MAX_VALUE, processingTimestamp = 0;

        for(Tuple tuple: tuples) {
            TaskEvent taskEvent = (TaskEvent) tuple.getValueByField("taskEvent");
            if(taskEvent.timestamp <= minTaskTimestamp) {
                minTaskTimestamp = taskEvent.timestamp;
                jobId = taskEvent.jobId;
            }
            sum += taskEvent.cpu;
            count++;

            e2eTimestamp = Math.min(e2eTimestamp, tuple.getLongByField("e2eTimestamp"));
            processingTimestamp = Math.max(processingTimestamp, tuple.getLongByField("processingTimestamp"));
        }
        float avgCpu = sum / count;
        collector.emit(new Values(new CPUPerJob(minTaskTimestamp, jobId, avgCpu), e2eTimestamp, processingTimestamp));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("cpuPerJob", "e2eTimestamp", "processingTimestamp"));
    }

}
