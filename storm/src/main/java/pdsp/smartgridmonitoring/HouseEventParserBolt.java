package pdsp.smartgridmonitoring;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import pdsp.utils.KafkaUtils;

import java.util.Map;


public class HouseEventParserBolt extends BaseWindowedBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        this.collector = collector;

    }

    @Override
    public void execute(TupleWindow tupleWindow) {

        long e2eTimestamp, processingTimestamp = 0;
        String line;

        for (Tuple tuple : tupleWindow.get()) {

            try {
                e2eTimestamp = tuple.getLongByField("e2eTimestamp");
                line = tuple.getString(0);
            } catch (IllegalArgumentException e) {
                String tupleStr = (String) tuple.getValue(4);
                Object[] arr = KafkaUtils.parseValue(tupleStr);
                line = (String) arr[0];
                e2eTimestamp = arr[1] != null ? (long) arr[1] : processingTimestamp;


            }

           // line = tuple.getStringByField("line");

            long tupleE2eTimestamp = tuple.getLongByField("e2eTimestamp");
            long tupleProcessingTimestamp = System.currentTimeMillis();
            e2eTimestamp = Math.min(e2eTimestamp, tupleE2eTimestamp);
            processingTimestamp = Math.max(processingTimestamp, tupleProcessingTimestamp);


            String[] fields = line.split(",");
            if (fields.length == 7) {

                long house_id = Long.parseLong(fields[6]);
                long household_id = Long.parseLong(fields[5]);
                long plug_id = Long.parseLong(fields[4]);
                double energyConsumption = Double.parseDouble(fields[2]);


                collector.emit(new Values(house_id, household_id, plug_id, energyConsumption, e2eTimestamp, processingTimestamp));


            } else {

                System.out.println("wrong input length");

            }

            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("house_id", "household_id", "plug_id", "energyConsumption", "e2eTimestamp", "processingTimestamp"));


    }
}
