package pdsp.TrafficMonitoring;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Map;

public class AvgSpeedCalculatorBolt extends BaseRichBolt {
    private OutputCollector collector;
    //save received roads
    private ArrayList<Road> roads = new ArrayList<Road>();
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            Road road = (Road) tuple.getValueByField("road");
            TrafficEvent event = (TrafficEvent) tuple.getValueByField("trafficEvent");
//            System.out.println("AvgSpeedCalculatorBolt received: " + "RoadID = " + road.getRoadId() + ", " + event);
            //check if road is already in the list
            boolean roadExists = false;
            for (Road r : roads) {
                if (r.getRoadId().equals(road.getRoadId())) {
//                    System.out.println("Road exists in list");
                    roadExists = true;
//                    System.out.println("Road id: " + r.getRoadId() + ", avg speed: " + r.getAverageSpeed() + ", total speed: " + r.getSpeed() + ", count: " + r.getCount());
                    r.addSpeed(event.getSpeed());
//                    System.out.println("Road id: " + r.getRoadId() + ", avg speed: " + r.getAverageSpeed() + ", total speed: " + r.getSpeed() + ", count: " + r.getCount());
                    road = r;   // update road with new speed
                    break;
                }
            }
            if (!roadExists) {
//                System.out.println("Road does not exist in list");
                road.addSpeed(event.getSpeed());
                roads.add(road);
            }
            roadExists = false;
            // emit road with updated speed
            collector.emit(new Values(road, event, tuple.getLongByField("e2eTimestamp"), tuple.getLongByField("processingTimestamp")));

//            System.out.println("road id: " + road.getRoadId() + ", avg speed: " + road.getAverageSpeed() + ", total speed: " + road.getSpeed() + ", count: " + road.getCount());

        }
        catch (Exception e) {
            System.out.println("Error in AvgSpeedCalculatorBolt: " + e);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("road", "trafficEvent", "e2eTimestamp", "processingTimestamp"));
    }
}
