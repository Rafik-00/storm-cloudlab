package pdsp.bargainIndex;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.windowing.TupleWindow;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class VWAPBolt extends BaseWindowedBolt {
    private OutputCollector collector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        String symbol = null;
        double totalPrice = 0.0;
        long totalVolume = 0;
        double quotePrice = 0.0;

        // Variables to hold min and max timestamps
        long e2eTimestamp = Long.MAX_VALUE;
        long processingTimestamp = 0;

        // Iterate over tuples in the window
        for (Tuple tuple : inputWindow.get()) {
            QuoteDataModel quoteData = (QuoteDataModel) tuple.getValueByField("quoteData");

            String currentSymbol = quoteData.getSymbol();
            double close = quoteData.getClose();

            // Get the timestamps from the tuple
            long currentE2eTimestamp = tuple.getLongByField("e2eTimestamp");
            long currentProcessingTimestamp = tuple.getLongByField("processingTimestamp");

            // Update the min and max timestamps
            if (currentE2eTimestamp < e2eTimestamp) {
                e2eTimestamp = currentE2eTimestamp;
            }
            if (currentProcessingTimestamp > processingTimestamp) {
                processingTimestamp = currentProcessingTimestamp;
            }

            // Initialize symbol if null
            if (symbol == null) {
                symbol = currentSymbol;
            }

            // Check if the current symbol matches the symbol in the window
            if (symbol.equals(currentSymbol)) {
                totalPrice += quoteData.getClose() * quoteData.getVolume();
                totalVolume += quoteData.getVolume();
                quotePrice = quoteData.getClose(); // the close price of the last quote i.e. trade price
            } else {
                // Different symbol, emit previous symbol's VWAP
                double vwap = totalVolume == 0 ? 0 : totalPrice / totalVolume;

                // Emit the result tuple with timestamps
                collector.emit(new Values(symbol, vwap, totalVolume, quotePrice, e2eTimestamp, processingTimestamp));

                // Reset variables for the new symbol
                symbol = currentSymbol;
                totalPrice = quoteData.getClose() * quoteData.getVolume();
                totalVolume = quoteData.getVolume();
                quotePrice = quoteData.getClose();
            }
        }

        // Emit VWAP for the last symbol in the window
        double vwap = totalVolume == 0 ? 0 : totalPrice / totalVolume;

        // Emit the result tuple with timestamps
        collector.emit(new Values(symbol, vwap, totalVolume, quotePrice, e2eTimestamp, processingTimestamp));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("symbol", "vwap", "totalVolume", "quotePrice", "e2eTimestamp", "processingTimestamp"));
    }
}
