package pdsp.spikedetection;

import java.time.format.DateTimeFormatter;
import java.util.Map;

import java.time.format.DateTimeFormatterBuilder;
import java.time.LocalDateTime;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SensorMeasurementParserBolt extends BaseRichBolt {
   private OutputCollector collector;
   private static final DateTimeFormatter formatterMillis = new DateTimeFormatterBuilder()
       .appendPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
       .toFormatter();
   private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

   @Override
   public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
       this.collector = outputCollector;
       System.out.println("Preparing ParserBolt");
   }

   @Override
   public void execute(Tuple tuple) {
       long processingTimestamp = System.currentTimeMillis();
       String value = tuple.getString(0);
       String[] fields = value.split("\\s+");
       LocalDateTime date = null;

       String dateStr = fields[0] + " " + fields[1];
       try {
           date = LocalDateTime.parse(dateStr, formatterMillis);
       } catch (Exception ex) {
           try {
               date = LocalDateTime.parse(dateStr, formatter);
           } catch (Exception ex2) {
               System.out.println("Error parsing record date/time field, input record: " + value);
               return;
           }
       }

       try {
           SensorMeasurementModel measurement = new SensorMeasurementModel(
               date.toEpochSecond(java.time.ZoneOffset.UTC), // timestamp
               Integer.parseInt(fields[2]), // sensorId
               Float.parseFloat(fields[3]), // temperature
               Float.parseFloat(fields[4]), // humidity
               Float.parseFloat(fields[5]), // light
               Float.parseFloat(fields[6])  // voltage
           );
           System.out.println("Emitting measurement: " + measurement.toString());
           int sensorId = measurement.getSensorId();

           collector.emit(new Values(sensorId, measurement,tuple.getValueByField("e2eTimestamp"), processingTimestamp));
           collector.ack(tuple);
       } catch (Exception e) {
           System.out.println("Error parsing record fields, input record: " + value);
       }
   }

   @Override
   public void declareOutputFields(OutputFieldsDeclarer declarer) {
       declarer.declare(new Fields("sensorId","measurement","e2eTimestamp", "processingTimestamp"));
   }

   @Override
   public void cleanup() {
   }

}
