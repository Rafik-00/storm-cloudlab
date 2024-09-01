package pdsp.machineOutlier;

import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseWindowedBolt;
import pdsp.common.AbstractTopology;
import pdsp.common.LoggerBolt;
import pdsp.config.Config;

public class MachineOutlierTopology extends AbstractTopology {
    int slidingWindowSize;
    int slidingWindowSlide;
    double threshold;

    public MachineOutlierTopology(String topologyName, String mode, String filePath, String kafkaTopic, int slidingWindowSize, int slidingWindowSlide, double threshold, Config config) {
        super(topologyName, mode, filePath, kafkaTopic, config);
        this.slidingWindowSize = slidingWindowSize;
        this.slidingWindowSlide = slidingWindowSlide;
        this.threshold = threshold;
    }

    @Override
    protected void buildTopology() {
        BaseRichSpout spout = getSpout();

        int parserBoltParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int outlierBoltParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        this.parallelism = (int) Math.round((parserBoltParallelism+outlierBoltParallelism) / 2.0);
        System.out.println("Parallelism in TOPOLOGY: " + this.parallelism);

        builder.setSpout("machine-usage-spout", spout);
        builder.setBolt("machine-usage-parser-bolt", new MachineUsageParserBolt(), parserBoltParallelism).shuffleGrouping("machine-usage-spout");
        builder.setBolt(
                "machine-outlier-bolt",
                new MachineOutlierBolt(threshold).withWindow(
                        new BaseWindowedBolt.Count(slidingWindowSize),
                        new BaseWindowedBolt.Count(slidingWindowSlide)
                ),
                outlierBoltParallelism
        ).shuffleGrouping("machine-usage-parser-bolt");
        builder.setBolt("logger-bolt", new LoggerBolt())
                .shuffleGrouping("machine-outlier-bolt");
    }
}
