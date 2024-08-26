package pdsp.tpch;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class SumBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<Integer, Integer> counts;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.counts = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        Integer orderPriority = input.getIntegerByField("orderPriority");
        Integer count = input.getIntegerByField("count");
        counts.put(orderPriority, counts.getOrDefault(orderPriority, 0) + count);
        collector.emit(new Values(orderPriority, counts.get(orderPriority), input.getValueByField("e2eTimestamp"), input.getValueByField("processingTimestamp")));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("orderPriority", "count", "e2eTimestamp", "processingTimestamp"));
    }
}
