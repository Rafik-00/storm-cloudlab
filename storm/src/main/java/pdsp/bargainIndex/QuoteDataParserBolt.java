package pdsp.bargainIndex;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class QuoteDataParserBolt extends BaseRichBolt {
    private OutputCollector collecter;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collecter = outputCollector;
        System.out.println("preparing parser");
    }

    @Override
    public void execute(Tuple tuple) {
        long processingTimestamp = System.currentTimeMillis();
        String line = tuple.getString(0);
        String[] fields = line.split(",");
        System.out.println("parsing line: " + line);
        System.out.println("fields: " + fields[0] + " " + fields[1] + " " + fields[2] + " " + fields[3] + " " + fields[4] + " " + fields[5] + " " + fields[6] + " " + fields[7]);    
        String symbol = fields[0];
        String date = fields[1];
        double open = Double.parseDouble(fields[2]);
        double high = Double.parseDouble(fields[2]);

        double low = Double.parseDouble(fields[4]);
        double close = Double.parseDouble(fields[5]);
        double adjClose = Double.parseDouble(fields[6]);
        long volume = Long.parseLong(fields[7]);
        QuoteDataModel quoteData = new QuoteDataModel(symbol, date, open, high, low, close, adjClose, volume);
        collecter.emit(new Values(symbol,quoteData, tuple.getValueByField("e2eTimestamp"), processingTimestamp));
        collecter.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("symbol","quoteData","e2eTimestamp","processingTimestamp"));
    }
}
