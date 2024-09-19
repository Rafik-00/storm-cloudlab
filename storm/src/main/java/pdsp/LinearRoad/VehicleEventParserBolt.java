package pdsp.LinearRoad;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import pdsp.utils.KafkaUtils;

import java.util.Map;

// Function to parse the log data into LogEvent objects
public class VehicleEventParserBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {


        long processingTimestamp = System.currentTimeMillis();
        long e2eTimestamp;

        String line;

        //String line = tuple.getStringByField("line");


        try {
            e2eTimestamp = tuple.getLongByField("e2eTimestamp");
            line = tuple.getString(0);
        } catch (IllegalArgumentException e) {
            String tupleStr = (String) tuple.getValue(4);
            Object[] arr = KafkaUtils.parseValue(tupleStr);
            line = (String) arr[0];
            e2eTimestamp = arr[1] != null ? (long) arr[1] : processingTimestamp;
        }

        // Parse the log data and create a LogEvent object
        // You may need to customize this logic based on the log format
        // Example: "54.36.149.41 - - [22/Jan/2019:03:56:14 +0330] "GET /filter/27|13%20%D9%85%DA%AF%D8%A7%D9%BE%DB%8C%DA%A9%D8%B3%D9%84,27|%DA%A9%D9%85%D8%AA%D8%B1%20%D8%A7%D8%B2%205%20%D9%85%DA%AF%D8%A7%D9%BE%DB%8C%DA%A9%D8%B3%D9%84,p53 HTTP/1.1" 200 30577 "-" "Mozilla/5.0 (compatible; AhrefsBot/6.1; +http://ahrefs.com/robot/)" "-"
        String[] fields = line.split(",");
        if (fields.length == 15) {

            int type = Integer.parseInt(fields[0]);
            long time = Long.parseLong(fields[1]);
            int vehicleId = Integer.parseInt(fields[2]);
            int speed = Integer.parseInt(fields[3]);
            int xway = Integer.parseInt(fields[4]);
            int lane = Integer.parseInt(fields[5]);
            int direction = Integer.parseInt(fields[6]);
            int segment = Integer.parseInt(fields[7]);
            int position = Integer.parseInt(fields[8]);
            int qid = Integer.parseInt(fields[9]);
            int sinit = Integer.parseInt(fields[10]);
            int send = Integer.parseInt(fields[11]);
            int dow = Integer.parseInt(fields[12]);
            int tod = Integer.parseInt(fields[13]);
            int day = Integer.parseInt(fields[14]);


            collector.emit(new Values(type, time, vehicleId, speed, xway, lane, direction, segment, position, qid, sinit, send, dow, tod, day, e2eTimestamp, processingTimestamp));


        } else {

            System.out.println( "wrong input length");

            collector.emit(new Values(0, 0L, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, e2eTimestamp, processingTimestamp));
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("type", "time", "vehicleID", "speed", "xway", "lane", "direction", "segment", "position", "qid", "sinit", "send", "dow", "tod", "day", "e2eTimestamp", "processingTimestamp"));
    }
}