package pdsp.LinearRoad;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


import java.util.*;

public class AccidentDetectionBolt extends BaseRichBolt {

    Map<String, List<Integer>> uniquePositions;
    OutputCollector collector;


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        this.collector = collector;
        this.uniquePositions = new HashMap<>();


    }

    @Override
    public void execute(Tuple input) {

        int type = input.getIntegerByField("type");
        int xway = input.getIntegerByField("xway");
        int lane = input.getIntegerByField("lane");
        int direction = input.getIntegerByField("direction");
        int segment = input.getIntegerByField("segment");
        int position = input.getIntegerByField("position");
        long time = input.getLongByField("time");


        // type 0 == a position report can lead to an accident
        if (type == 0) {
            String accidentID = "ID" + xway + lane + direction + segment + position;

            int vehicleId = input.getIntegerByField("vehicleID");

            List<Integer> vehiclesInvolved = uniquePositions.get(accidentID);

            //put new vehicle to vehicles involved
            if (vehiclesInvolved == null) {
                vehiclesInvolved = new ArrayList<>();

            }

            if (!vehiclesInvolved.contains(vehicleId)) {
                vehiclesInvolved.add(vehicleId);

            }

            uniquePositions.put(accidentID, vehiclesInvolved);

            // if there's more than one vehicle at same place at the same time there is an accident
            if (vehiclesInvolved.size() > 1) {
                AccidentNotification accidentNotification = new AccidentNotification(

                        lane,
                        position,
                        time,
                        segment);

                collector.emit(new Values(accidentNotification, vehiclesInvolved, input.getLongByField("e2eTimestamp"), input.getLongByField("processingTimestamp")));

                System.out.println("accident detected " + accidentID);

            }

            collector.ack(input);
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("accidentNotification", "vehiclesInvolved", "e2eTimestamp", "processingTimestamp"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return Map.of();
    }
}
