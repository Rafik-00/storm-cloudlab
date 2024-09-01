package pdsp.tpch;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import pdsp.utils.KafkaUtils;


public class TPCHEventParserBolt extends BaseRichBolt{
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        System.out.println("Preparing ParserBolt");
    }

    @Override
    public void execute(Tuple tuple) {
        long processingTimestamp = System.currentTimeMillis();
        long e2eTimestamp;
        String line;
        try {
            e2eTimestamp = tuple.getLongByField("e2eTimestamp");
            line = tuple.getString(0);
        } catch (Exception e) {
            String tupleStr = (String) tuple.getValue(4);
            Object[] arr = KafkaUtils.parseValue(tupleStr);
            line = (String) arr[0];
            e2eTimestamp = arr[1] != null ? (long) arr[1] : processingTimestamp;
        }
        String[] fields = line.split("\\s+");

        String orderKey  = fields[0];
        String cname = fields[1];
        String caddress = fields[2];
        int orderPriority = Integer.parseInt(fields[3]);
        double extendedPrice = Double.parseDouble(fields[4]);
        double discount = Double.parseDouble(fields[5]);
        TPCHEventModel event = new TPCHEventModel(orderKey, cname, caddress, orderPriority, extendedPrice, discount);
        collector.emit(new Values(event, e2eTimestamp, processingTimestamp));
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tpchEvent","e2eTimestamp","processingTimestamp"));
    }
    
}
