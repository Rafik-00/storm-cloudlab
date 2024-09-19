package pdsp.LinearRoad;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DailyExpenditureCalculatorBolt extends CalculatorBolt {

    OutputCollector collector;
    long tollPerSegment = -1;
    boolean vehiclesArrived = false;
    List<Integer> vehicleIDs;

    //vehicles -> expenditure
    private Map<Integer, Long> vehicleTotalExpenditureState;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        this.collector = collector;
        vehicleTotalExpenditureState = new HashMap<>();
        this.vehicleIDs = new ArrayList<>();


    }

    @Override
    public void execute(Tuple input) {

        // a CalculatedToll arrives
        if (input.contains("toll")) {

            //initialize or update tollPerSegment
            this.tollPerSegment = input.getIntegerByField("toll");

            // if vehicles have already arrived but the toll was not calculated yet: add vehicle and toll to total Expenditure
            if (vehiclesArrived) {
                for (int id : vehicleIDs) {
                    long toll = vehicleTotalExpenditureState.getOrDefault(id, 0L);
                    long totalToll = toll + tollPerSegment;

                    vehicleTotalExpenditureState.put(id, totalToll);
                    vehiclesArrived = false;
                    collector.emit(new Values(id, vehicleTotalExpenditureState.get(id), input.getLongByField("e2eTimestamp"), input.getLongByField("processingTimestamp")));
                }

            }

            collector.ack(input);

        }
        // a vehicle arrives
        else {

            int vehicleID = input.getIntegerByField("vehicleID");

            // tollpersegment is not initialized yet: store vehicle ID
            if (tollPerSegment == -1) {
                this.vehicleIDs.add(vehicleID);
                this.vehiclesArrived = true;
            } else {

                vehicleTotalExpenditureState.put(vehicleID, vehicleTotalExpenditureState.get(vehicleID) + tollPerSegment);
            }

            collector.emit(new Values(vehicleID, vehicleTotalExpenditureState.get(vehicleID), input.getLongByField("e2eTimestamp"), input.getLongByField("processingTimestamp")));
        }


    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("vehicleID", "totalExpenditure", "e2eTimestamp", "processingTimestamp"));

    }
}
