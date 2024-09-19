package pdsp.LinearRoad;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class CreateTollNotificationBolt extends BaseRichBolt {

    OutputCollector collector;
    int toll;
    String segment;
    int vehicleID;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        //toll is not calculated yet, segment and vehicle id are not initialized yet
        this.toll = -1;
        this.segment = "notinitialized";
        this.vehicleID = -1;
    }

    @Override
    public void execute(Tuple input) {

        if (input.contains("segmentID")) {
            segment = input.getStringByField("segmentID");
            toll = input.getIntegerByField("toll");
        }

        if (input.contains("vehicleID")) {

            vehicleID = input.getIntegerByField("vehicleID");


            TollNotification notification = new TollNotification(vehicleID, segment, toll);

            System.out.println(notification.stringFormatter());

            collector.emit(new Values(notification, input.getLongByField("e2eTimestamp"), input.getLongByField("processingTimestamp")));
            collector.ack(input);

            vehicleID = -1;


        }


    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("tollNotification", "e2eTimestamp", "processingTimestamp"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return Map.of();
    }
}
