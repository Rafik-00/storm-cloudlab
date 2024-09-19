package pdsp.smartgridmonitoring;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class GlobalMedianBolt extends CalculatorBolt {

    private List<Double> energyConsumptions;


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        this.energyConsumptions = new ArrayList<>();
    }

    @Override
    public void execute(Tuple input) {


        long house_id = input.getLongByField("house_id");
        double energyConsumption = input.getDoubleByField("energyConsumption");

        energyConsumptions.add(energyConsumption);


        double median = calculateMedian(energyConsumptions);

        collector.emit(new Values(house_id,median,input.getLongByField("e2eTimestamp"), input.getLongByField("processingTimestamp")));

        collector.ack(input);


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("house_id", "globalMedian","e2eTimestamp", "processingTimestamp"));
    }
}
