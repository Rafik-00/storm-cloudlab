package pdsp.tpch;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class PriorityMapperBolt extends BaseRichBolt {
    private OutputCollector collector;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple input) {
        // Assuming TPCHEvent is a POJO and we can get it from the input tuple
        TPCHEventModel tpchEvent = (TPCHEventModel) input.getValueByField("filteredEvent");
        Integer orderPriority = tpchEvent.getOrderPriority();

        // Emit the result
        collector.emit(new Values(orderPriority, 1, input.getValueByField("e2eTimestamp"), input.getValueByField("processingTimestamp")));

        // Acknowledge the tuple
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("orderPriority", "count", "e2eTimestamp", "processingTimestamp"));
    }

    @Override
    public void cleanup() {
        // Any cleanup code can go here
    }
}

