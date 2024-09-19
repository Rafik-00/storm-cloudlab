package pdsp.LinearRoad;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class CalculateTimeTravelledBolt extends BaseRichBolt {

    OutputCollector collector;
    Map<Integer, Long> starttimePerVehicleMap;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        this.collector = collector;
        this.starttimePerVehicleMap = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {

        long time = input.getLongByField("time");
        int vehicleID = input.getIntegerByField("vehicleID");

        if (!starttimePerVehicleMap.containsKey(vehicleID))
            starttimePerVehicleMap.put(vehicleID, time);

        long timeTravelled = (time - starttimePerVehicleMap.get(vehicleID));

        collector.emit(new Values(vehicleID, timeTravelled,input.getLongByField("e2eTimestamp"), input.getLongByField("processingTimestamp")));
        collector.ack(input);


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("vehicleID", "timeTravelled", "e2eTimestamp", "processingTimestamp"));

    }
}
