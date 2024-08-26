package pdsp.machineOutlier;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseWindowedBolt;
import pdsp.common.AbstractTopology;
import pdsp.common.CustomKafkaSpout;
import pdsp.common.FileSpout;
import pdsp.common.LoggerBolt;

public class MachineOutlierTopology extends AbstractTopology {
    int slidingWindowSize;
    int slidingWindowSlide;
    double threshold;

    public MachineOutlierTopology(String topologyName, String mode, String filePath, String kafkaTopic, int slidingWindowSize, int slidingWindowSlide, double threshold) {
        super(topologyName, mode, filePath, kafkaTopic);
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
