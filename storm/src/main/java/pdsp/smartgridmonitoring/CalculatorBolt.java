package pdsp.smartgridmonitoring;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * Calculates the global median and average of the energycomsumption
 */


public class CalculatorBolt extends BaseRichBolt {
    OutputCollector collector;
    double previousaverageload;


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.previousaverageload = 0;


    }

    @Override
    public void execute(Tuple houseEvent) {


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("median", "average"));
    }

    double calculateAverage(List<Double> energyConsumptions) {

        double average;

        double sum = 0;
        int size = energyConsumptions.size();

        for (double energyConsumption : energyConsumptions) {

            sum += energyConsumption;

        }

        average = sum / size;

        return average;


    }

    double calculateMedian(List<Double> energyConsumptions) {

        Collections.sort(energyConsumptions);

        double median;
        int size = energyConsumptions.size();
        if (size % 2 == 0) {

            median = (energyConsumptions.get(size / 2 - 1) + energyConsumptions.get(size / 2)) / 2.0;
        } else {

            median = energyConsumptions.get(size / 2);
        }

        return median;
    }


    double loadPrediction(double median, double average) {


        return ( previousaverageload + median + average) / 2;
    }
}