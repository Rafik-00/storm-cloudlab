package pdsp.sentimentAnalysis;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import pdsp.utils.KafkaUtils;

import java.util.Map;

public class TweetParserBolt extends BaseRichBolt {
	private OutputCollector collector;

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

	@Override
	public void execute(Tuple input) {
		long processingTimestamp = System.currentTimeMillis();
		long e2eTimestamp;

		try {
			JsonObject obj = JsonParser.parseString((String) input.getValue(0)).getAsJsonObject();
			String id = obj.get("id").getAsString();
			String text = obj.get("text").getAsString();
			String timestamp = obj.get("created_at").getAsString();
			e2eTimestamp = input.getLongByField("e2eTimestamp");

			collector.emit(new Values(id, timestamp, text, e2eTimestamp, processingTimestamp));
			System.out.println("Tweet: " + text + " - ID: " + id + " - Timestamp: " + timestamp);
		} catch (Exception e) {
			try {
				String tupleStr = (String) input.getValue(4);
				Object[] arr = KafkaUtils.parseValue(tupleStr);
				String line = (String) arr[0];

				JsonObject obj = JsonParser.parseString(line).getAsJsonObject();
				String id = obj.get("id").getAsString();
				String text = obj.get("text").getAsString();
				String timestamp = obj.get("created_at").getAsString();
				e2eTimestamp = arr[1] != null ? (long) arr[1] : System.currentTimeMillis();

				collector.emit(new Values(id, timestamp, text, e2eTimestamp, processingTimestamp));
				System.out.println("Tweet: " + text + " - ID: " + id + " - Timestamp: " + timestamp);
			}
			catch (Exception e1) {
				e1.printStackTrace();
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("id", "timestamp", "text", "e2eTimestamp", "processingTimestamp"));
	}

}
