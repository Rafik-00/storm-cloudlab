package pdsp.googleCloudMonitoring;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import pdsp.utils.KafkaUtils;

import java.util.Map;

public class TaskEventParserBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        long processingTimestamp = System.currentTimeMillis();
        long e2eTimestamp;
        String[] values;
        try {
            values = tuple.getStringByField("line").split(",");
            e2eTimestamp = (long) tuple.getValueByField("e2eTimestamp");
        } catch (Exception e) {
            try {
                String tupleStr = (String) tuple.getValue(4);
                Object[] tupleValues = KafkaUtils.parseValue(tupleStr);
                String line = (String) tupleValues[0];
                values = line.split(",");
                e2eTimestamp = (long) tupleValues[1];
            } catch (Exception e2) {
                e2.printStackTrace();
                return; // terminate function, emit nothing
            }
        }
        TaskEvent taskEvent = new TaskEvent(
                values[2].isEmpty() ? 0 : Long.parseLong(values[2]),
                values[0].isEmpty() ? 0 : Long.parseLong(values[0]),
                values[1].isEmpty() ? 0 : Long.parseLong(values[1]),
                values[4].isEmpty() ? 0 : Long.parseLong(values[4]),
                values[18].isEmpty() ? 0 : Integer.parseInt(values[18]),
                values[15].isEmpty() ? 0 : (int) Float.parseFloat(values[15]),
                values[3].isEmpty() ? 0 : Integer.parseInt(values[3]),
                values[7].isEmpty() ? 0 : (int) Float.parseFloat(values[7]),
                values[8].isEmpty() ? 0 : Float.parseFloat(values[8]),
                values[9].isEmpty() ? 0 : Float.parseFloat(values[9]),
                values[12].isEmpty() ? 0 : Float.parseFloat(values[12]),
                values[17].isEmpty() ? 0 : Integer.parseInt(values[17])
        );
        collector.emit(new Values(taskEvent, taskEvent.getJobId(), taskEvent.getCategory(), e2eTimestamp, processingTimestamp));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("taskEvent", "jobId", "category", "e2eTimestamp", "processingTimestamp"));
    }
}
