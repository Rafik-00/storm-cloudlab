package pdsp.bargainIndex;

import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import pdsp.common.AbstractTopology;
import pdsp.common.LoggerBolt;
import pdsp.config.Config;

public class BargainIndexTopology extends AbstractTopology {

    private double threshold;

    public BargainIndexTopology(String topologyName, String mode, String filePath, String kafkaTopic, double threshold, Config config) {
        super(topologyName ,mode, filePath, kafkaTopic, config);
        this.threshold = threshold;
    }

    @Override
    protected void buildTopology() {
        BaseRichSpout spout = getSpout();

        int parserParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int vwapCalculatorParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int bargainIndexParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        
        // Define the topology
        builder.setSpout("source-spout", spout);
        builder.setBolt("parser-bolt", new QuoteDataParserBolt(), parserParallelism).shuffleGrouping("source-spout");
        builder.setBolt("vwap-calculator-bolt", new VWAPBolt().withWindow(
                        new BaseWindowedBolt.Count(windowSize),
                        new BaseWindowedBolt.Count(slidingInterval)
                ), vwapCalculatorParallelism)
                .fieldsGrouping("parser-bolt", new Fields("symbol"));
        builder.setBolt("bargain-index-bolt", new BargainIndexCalculatorBolt(threshold), bargainIndexParallelism)
                .fieldsGrouping("vwap-calculator-bolt", new Fields("symbol"));
        builder.setBolt("logger-bolt", new LoggerBolt(), 1).shuffleGrouping("bargain-index-bolt");
    }
}
