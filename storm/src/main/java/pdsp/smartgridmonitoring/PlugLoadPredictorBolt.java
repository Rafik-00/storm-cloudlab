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

public class PlugLoadPredictorBolt extends CalculatorBolt {

    List<Double> energyConsumptions;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.energyConsumptions = new ArrayList<>();
        this.collector = outputCollector;

    }

    @Override
    public void execute(Tuple input) {


        long house_id = input.getLongByField("house_id");
        long household_id = input.getLongByField("household_id");
        long plug_id = input.getLongByField("plug_id");

        String plugID = "PLUG " + house_id + household_id + plug_id;

        double energyConsumption = input.getDoubleByField("energyConsumption");

        energyConsumptions.add(energyConsumption);

        double median = calculateMedian(energyConsumptions);
        double average = calculateAverage(energyConsumptions);
        double predicion = loadPrediction(median,average);

        collector.emit(new Values(plugID, predicion,input.getLongByField("e2eTimestamp"), input.getLongByField("processingTimestamp")));
        collector.ack(input);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("plugID", "plugLoadPrediction","e2eTimestamp", "processingTimestamp"));

    }
}
