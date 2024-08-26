package pdsp.common;

public class Constants {
    interface QueryLabels {
        String PRODUCER_INTERVAL = "producer-interval";
        String INGESTION_INTERVAL = "ingestion-interval";
        String THROUGHPUT = "throughput";
        String TPT_MEAN = "throughput-mean";
        String E2E_MEAN = "e2e-mean";
        String PROC_MEAN = "proc-mean";
        String PROCLATENCY = "proc-latency";
        String COUNTER = "counter";
    }

    interface QueryProperties {
        String DURATION = "duration";
        String QUERY = "query";
        String PARALLELISM_HINT = "parallelism-hint";
    }
}