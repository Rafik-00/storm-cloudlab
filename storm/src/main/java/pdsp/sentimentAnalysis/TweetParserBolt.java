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
		try {
			JsonObject obj = JsonParser.parseString((String) input.getValue(0)).getAsJsonObject();
			String id = obj.get("id").getAsString();
			String text = obj.get("text").getAsString();
			String timestamp = obj.get("created_at").getAsString();

			collector.emit(new Values(id, timestamp, text, input.getLongByField("e2eTimestamp"), processingTimestamp));
			System.out.println("Tweet: " + text + " - ID: " + id + " - Timestamp: " + timestamp);
		} catch (Exception e) {
			e.printStackTrace();
			try {
				JsonObject obj = JsonParser.parseString((String) input.getValue(4)).getAsJsonObject();
				String id = obj.get("id").getAsString();
				String text = obj.get("text").getAsString();
				String timestamp = obj.get("created_at").getAsString();

				collector.emit(new Values(id, timestamp, text, input.getLongByField("e2eTimestamp"), processingTimestamp));
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
