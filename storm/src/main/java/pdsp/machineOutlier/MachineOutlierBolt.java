package pdsp.machineOutlier;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static pdsp.machineOutlier.BFPRTAlgorithm.bfprt;

public class MachineOutlierBolt extends BaseWindowedBolt {
    private OutputCollector outputCollector;
    private static final Logger LOG = LoggerFactory.getLogger(MachineOutlierBolt.class);
    private final double threshold;

    public MachineOutlierBolt(double threshold) {
        this.threshold = threshold;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        List<Tuple> tuples = tupleWindow.get();
        List<MachineUsage> allMachines = new ArrayList<>();
        List<Long> allE2eTimestamps = new ArrayList<>();
        List<Long> allProcessingTimestamps = new ArrayList<>();

        long e2eTimestamp = Long.MAX_VALUE, processingTimestamp = 0;

        // TODO: IS THIS CORRECT? Where does the string variable s come from? Look at Flink implementation
        for(Tuple tuple : tuples){
            MachineUsage machineUsage = (MachineUsage) tuple.getValueByField("machineUsage");
            allMachines.add(machineUsage);
            allE2eTimestamps.add(tuple.getLongByField("e2eTimestamp"));
            allProcessingTimestamps.add(tuple.getLongByField("processingTimestamp"));
        }

        List<MachineUsage> machineUsages = new ArrayList<>();
        List<Long> e2eTimestamps = new ArrayList<>();
        List<Long> processingTimestamps = new ArrayList<>();
        for(int i = 0; i < allMachines.size(); i++){
            MachineUsage machineUsage = allMachines.get(i);
            if (machineUsage != null) {
                machineUsages.add(machineUsage);
                e2eTimestamps.add(allE2eTimestamps.get(i));
                processingTimestamps.add(allProcessingTimestamps.get(i));
            }
        }

        // Apply the BFPRT algorithm to detect abnormal readings
        double threshold = 0.5;
        // Check if the input list is empty or contains only one element
        if (machineUsages.isEmpty() || machineUsages.size() == 1) {
            // No outliers can be detected
            return;
        }

        // Convert the list of MachineUsage objects to an array for easy manipulation
        MachineUsage[] arr = machineUsages.toArray(new MachineUsage[0]);

        // Find the kth element using the BFPRT algorithm
        int k = (int) Math.ceil(arr.length * 0.75);  // Choose the value of k as desired (e.g., 75%)
        MachineUsage kthElement = bfprt(arr, 0, arr.length - 1, k);

        // Iterate over the array and collect the abnormal readings
        for (int i = 0; i < arr.length; i++) {
            MachineUsage machineUsage = arr[i];
            LOG.info("threshold value euclidean" + machineUsage.getEuclideanDistance(kthElement));
            if (machineUsage.getEuclideanDistance(kthElement) > threshold) {
                // Emit the outlier as a string
                outputCollector.emit(new Values(machineUsage.getMachineId(),
                        machineUsage.getTimestamp(),
                        machineUsage.getCpuUtilPercentage(),
                        machineUsage.getMemUtilPercentage(),
                        machineUsage.getMissPerThousandInstructions(),
                        machineUsage.getNetIn(),
                        machineUsage.getNetOut(),
                        machineUsage.getDiskIO(),
                        e2eTimestamps.get(i),
                        processingTimestamps.get(i))
                );
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
                "diskIO",
                "e2eTimestamp",
                "processingTimestamp"
        ));
    }
}
