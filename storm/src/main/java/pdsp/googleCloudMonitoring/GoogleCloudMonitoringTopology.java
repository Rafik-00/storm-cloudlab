package pdsp.googleCloudMonitoring;

import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import pdsp.common.AbstractTopology;
import pdsp.common.LoggerBolt;
import pdsp.config.Config;

public class GoogleCloudMonitoringTopology extends AbstractTopology {

    public GoogleCloudMonitoringTopology(String topologyName, String mode, String filePath, String kafkaTopic, Config config) {
        super(topologyName, mode, filePath, kafkaTopic, config);
    }

    @Override
    protected void buildTopology() {
        BaseRichSpout spout = getSpout();
        int taskEventParserBoltParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int cpuPerJobBoltParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int cpuPerCategoryBoltParallelism = this.parallelismEnumerator.getRandomParallelismHint();

        this.parallelism = (int) Math.round((taskEventParserBoltParallelism + cpuPerJobBoltParallelism + cpuPerCategoryBoltParallelism) / 3.0);
        System.out.println("Parallelism in TOPOLOGY: " + this.parallelism);
        config.registerSerialization(pdsp.googleCloudMonitoring.CPUPerCategory.class);
        config.registerSerialization(pdsp.googleCloudMonitoring.CPUPerJob.class);
        config.registerSerialization(pdsp.googleCloudMonitoring.TaskEvent.class);

        builder.setSpout("gc-monitoring-spout", spout);
        // original Flink implementation takes input to determine type of query (CPU per job or CPU per category)
        // here I am calculating both anyway, but we can add a similar input to determine which one to calculate
        builder.setBolt("task-event-parser-bolt", new TaskEventParserBolt(), taskEventParserBoltParallelism).shuffleGrouping("gc-monitoring-spout");
        builder.setBolt(
                "cpu-per-job-bolt",
                new CPUPerJobBolt().withWindow(
                        new BaseWindowedBolt.Count(2), // window size = 2 tuples
                        new BaseWindowedBolt.Count(1)  // sliding interval every 1 tuple
                ),
                cpuPerJobBoltParallelism
        ).fieldsGrouping("task-event-parser-bolt", new Fields("jobId"));
        builder.setBolt(
                "cpu-per-category-bolt",
                new CPUPerCategoryBolt().withWindow(
                        new BaseWindowedBolt.Count(2), // window size = 2 tuples
                        new BaseWindowedBolt.Count(1)  // sliding interval every 1 tuple
                ),
                cpuPerCategoryBoltParallelism
        ).fieldsGrouping("task-event-parser-bolt", new Fields("category"));
        builder.setBolt("logger-bolt", new LoggerBolt())
                .shuffleGrouping("cpu-per-job-bolt")
                .shuffleGrouping("cpu-per-category-bolt");
    }
}
