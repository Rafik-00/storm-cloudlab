package pdsp.bargainIndex;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class BargainIndexCalculatorBolt extends BaseRichBolt {
    private OutputCollector collector;
    private double threshold;

    //add constructor that takes a threshold value
    public BargainIndexCalculatorBolt(double threshold) {
        this.threshold = threshold;
    }


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
       this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple input) {
        String symbol = input.getStringByField("symbol");
        double vwap = input.getDoubleByField("vwap");
        long totalVolume = input.getLongByField("totalVolume");
        double quotePrice = input.getDoubleByField("quotePrice");

        // Calculate the bargain index based on the relationship between the quote price and VWAP
        double bargainIndex = calculateBargainIndex(quotePrice, vwap);

        // Emit the result tuple containing the symbol, bargain index, and any other relevant information
        if(isBargain(bargainIndex)) {
            collector.emit(new Values(symbol, bargainIndex, totalVolume,input.getLongByField("e2eTimestamp"), input.getLongByField("processingTimestamp")));
        }
    }

    private double calculateBargainIndex(double quotePrice, double vwap) {
        if (quotePrice > vwap) {
            return quotePrice / vwap;
        } else {
            return 0;
        }
    }
    private boolean isBargain(double bargainIndex) {
        return bargainIndex > threshold;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("symbol", "bargainIndex", "totalVolume","e2eTimestamp", "processingTimestamp"));
    }


}
