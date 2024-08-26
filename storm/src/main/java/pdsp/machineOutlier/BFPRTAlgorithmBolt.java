package pdsp.machineOutlier;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;

import static pdsp.machineOutlier.BFPRTAlgorithm.bfprt;

public class BFPRTAlgorithmBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private static final double THRESHOLD = 0.5;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        List<MachineUsage> machineUsages = (List<MachineUsage>) tuple.getValueByField("machineUsages");

        if (machineUsages == null || machineUsages.isEmpty() || machineUsages.size() == 1) {
            return;
        }

        MachineUsage[] arr = machineUsages.toArray(new MachineUsage[0]);
        int k = (int) Math.ceil(arr.length * 0.75);
        MachineUsage kthElement = bfprt(arr, 0, arr.length - 1, k);

        for (MachineUsage machineUsage : arr) {
            if (machineUsage.getEuclideanDistance(kthElement) > THRESHOLD) {
                outputCollector.emit(new Values(
                        machineUsage.getMachineId(),
                        machineUsage.getTimestamp(),
                        machineUsage.getCpuUtilPercentage(),
                        machineUsage.getMemUtilPercentage(),
                        machineUsage.getMissPerThousandInstructions(),
                        machineUsage.getNetIn(),
                        machineUsage.getNetOut(),
                        machineUsage.getDiskIO()
                ));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(
                "machineId",
                "timestamp",
                "cpuUtilPercentage",
                "memUtilPercentage",
                "missPerThousandInstructions",
                "netIn",
                "netOut",
                "diskIO"
        ));
    }
}
