package pdsp.tpch;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class FormatterOutputBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        Integer orderPriority = tuple.getIntegerByField("orderPriority");
        Integer count = tuple.getIntegerByField("count");
        String result = "orderPriority : " + orderPriority + " Number of orders: " + count;
        collector.emit(new Values(result, tuple.getValueByField("e2eTimestamp"), tuple.getValueByField("processingTimestamp")));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("result", "e2eTimestamp", "processingTimestamp"));
    }
}