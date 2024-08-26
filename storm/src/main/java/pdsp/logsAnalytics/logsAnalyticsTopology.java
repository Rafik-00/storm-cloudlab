package pdsp.logsAnalytics;

import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import pdsp.common.AbstractTopology;
import pdsp.common.FileSpout;
import org.apache.storm.Config;



public class logsAnalyticsTopology extends AbstractTopology {
    public logsAnalyticsTopology(String topologyName, String mode, String filePath, String kafkaTopic) {
        super(topologyName, mode, filePath, kafkaTopic);
    }


    @Override
    protected void buildTopology() {
        BaseRichSpout spout = getSpout();
        builder.setSpout("fileSpout", spout);
        builder.setBolt("logParserBolt", new logParserBolt(), parallelism).shuffleGrouping("fileSpout");
        builder.setBolt("statusCounterBolt", new statusCounterBolt(), parallelism).shuffleGrouping("logParserBolt");
        builder.setBolt("volumeCounterBolt", new volumeCounterBolt(), parallelism).shuffleGrouping("logParserBolt");

    }
}
