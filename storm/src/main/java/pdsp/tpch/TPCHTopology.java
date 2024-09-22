package pdsp.tpch;

import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

import pdsp.common.AbstractTopology;
import pdsp.common.LoggerBolt;
import pdsp.config.Config;

public class TPCHTopology extends AbstractTopology {
        public TPCHTopology(String topologyName, String mode, String filePath, String kafkaTopic, Config config) {
            super(topologyName, mode, filePath, kafkaTopic, config);
        }

        @Override
        protected void buildTopology(){
        BaseRichSpout spout = getSpout();

        int parserParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int filterCalculatorParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int priorityMapperParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int sumParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int formatterOutputParallelism = this.parallelismEnumerator.getRandomParallelismHint();

        parallelism = (int) Math.round((parserParallelism + filterCalculatorParallelism + priorityMapperParallelism + sumParallelism + formatterOutputParallelism) / 5.0);
        config.registerSerialization(pdsp.tpch.TPCHEventModel.class);

        // Define the topology
        builder.setSpout("fileSpout", spout);
        builder.setBolt("tpchEventParserBolt", new TPCHEventParserBolt(), parserParallelism).shuffleGrouping("fileSpout");
        builder.setBolt("filterCalculatorBolt", new FilterCalculatorBolt().withWindow(
                new BaseWindowedBolt.Count(windowSize), 
                new BaseWindowedBolt.Count(slidingInterval)  
        ), filterCalculatorParallelism).shuffleGrouping("tpchEventParserBolt");
        builder.setBolt("priorityMapperBolt", new PriorityMapperBolt(),priorityMapperParallelism).shuffleGrouping("filterCalculatorBolt");
        builder.setBolt("sumBolt", new SumBolt(), sumParallelism).fieldsGrouping("priorityMapperBolt", new Fields("orderPriority"));
        builder.setBolt("formatterOutputBolt", new FormatterOutputBolt(),formatterOutputParallelism).shuffleGrouping("sumBolt");
        builder.setBolt("logger-bolt", new LoggerBolt(), 1).shuffleGrouping("formatterOutputBolt");
        
        }
}
