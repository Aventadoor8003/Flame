package cis5550.tools;

import cis5550.generic.Coordinator;
import cis5550.kvs.KVSClient;
import cis5550.model.WorkerMeta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RddAgent {
    private final KVSClient kvsClient;
    private final String inputTable;
    private final Logger logger = Logger.getLogger(RddAgent.class);
    private final Map<String, String> additionalParams = new HashMap<>();
    private static int concurrencyLevel = 1;
    public RddAgent(KVSClient kvsClient, String inputTable) {
        this.kvsClient = kvsClient;
        this.inputTable = inputTable;
        logger.debug("Rdd util constructed. Input table: " + inputTable + " " + kvsClient.getCoordinator());
    }

    /**
     * Invokes the operation on the lambda on all flame workers
     * @param operation a single world, use lower camel case. example: flatMap, mapToPair, intersection, sample, groupBy
     *                  when submitted to flame workers, it will be concatenated into a route, example: /rdd/flatMap
     * @param rddType a single world, use lower camel case. Now only support: [rdd], [pairRdd]
     */
    public String invokeOperation(String operation, byte[] lambda, String rddType) throws IOException {
        logger.debug("[rdd utils] start");
        String outputTable = TableNameGenerator.generateUniqueTableName(operation);
        Partitioner partitioner = new Partitioner();

        //add all kvs workers
        int workerCnt = kvsClient.numWorkers();
        List<WorkerMeta> workers = new ArrayList<>(workerCnt);
        for(int i = 0; i < workerCnt; i++) {
            workers.add(new WorkerMeta(kvsClient.getWorkerAddress(i), kvsClient.getWorkerID(i)));
        }
        logger.debug("Got worker meta list from kvs client" + workers);

        partitioner.setKeyRangesPerWorker(concurrencyLevel);
        for(int i = 0; i < workerCnt - 1; i++) {
            WorkerMeta worker = workers.get(i);
            partitioner.addKVSWorker(worker.getAddress(), worker.getWorkerId(), workers.get(i + 1).getWorkerId());
        }
        partitioner.addKVSWorker(workers.get(workerCnt - 1).getAddress(),
                workers.get(workerCnt - 1).getWorkerId(),
                null);
        partitioner.addKVSWorker(workers.get(workerCnt - 1).getAddress(),
                null,
                workers.get(0).getWorkerId());


        //add all flame workers
        List<String> flameWorkers = Coordinator.getWorkers();
        for(String worker : flameWorkers) {
            partitioner.addFlameWorker(worker);
        }
        logger.debug("Got flame worker list from coordinator" + flameWorkers);
        List<Partitioner.Partition> partitions = partitioner.assignPartitions();
        for(Partitioner.Partition partition : partitions) {
            logger.debug("Partition: " + partition);
        }

        //upload lambda to all flame workers
        Thread[] threads = new Thread[partitions.size()];
        List<byte[]> results = new ArrayList<>();
        List<String> exceptions = new ArrayList<>();
        int tid = 0;
        for (Partitioner.Partition partition : partitions) {
            StringBuilder urlBuilder = new StringBuilder();
            urlBuilder.append("http://").append(partition.assignedFlameWorker).append(concatenateRoute(rddType, operation)).append("?");
            urlBuilder.append("inputTable=").append(inputTable).append("&");
            urlBuilder.append("outputTable=").append(outputTable).append("&");
            urlBuilder.append("kvs=").append(kvsClient.getCoordinator()).append("&");
            if(partition.fromKey != null) {
                urlBuilder.append("fromKey=").append(partition.fromKey).append("&");
            }
            if(partition.toKeyExclusive != null) {
                urlBuilder.append("toKey=").append(partition.toKeyExclusive).append("&");
            }
            if (urlBuilder.charAt(urlBuilder.length() - 1) == '&') {
                urlBuilder.deleteCharAt(urlBuilder.length() - 1);
            }
            for (Map.Entry<String, String> entry : additionalParams.entrySet()) {
                urlBuilder.append("&").append(entry.getKey()).append("=").append(entry.getValue());
            }
            String url = urlBuilder.toString();
            logger.debug("Url concatenated" + url);
            threads[tid] = new Thread("Dispatching " + operation + " #" + (tid + 1)) {
                public void run() {
                    try {
                        HTTP.Response response = HTTP.doRequest("POST", url, lambda);
                        if(response.statusCode() != 200) {
                            throw new RuntimeException("Failed to execute partition + " + partition + " with status code " + response.statusCode());
                        }
                        results.add(response.body());
                    } catch (Exception e) {
                        exceptions.add("Exception: " + e);
                    }
                }
            };
            threads[tid].start();
            tid++;
        }
        if(!exceptions.isEmpty()) {
            logger.error("Failed to upload lambda to all flame workers");
        }
        for(Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                logger.error("Failed to join: ", e);
            }
        }
        return outputTable;
    }

    public RddAgent addAdditionalParam(String key, String value) {
        additionalParams.put(key, value);
        return this;
    }

    public static void setConcurrencyLevel(int concurrencyLevel) {
        if(concurrencyLevel < 1) {
            throw new IllegalArgumentException("concurrencyLevel must be greater than 0");
        };
        RddAgent.concurrencyLevel = concurrencyLevel;
    }

    public String toString() {
        return "RddAgent{" +
                "kvsClient=" + kvsClient +
                ", inputTable='" + inputTable + '\'' +
                ", logger=" + logger +
                ", additionalParams=" + additionalParams +
                '}';
    }

    private String concatenateRoute(String rddType, String operation) {
        return "/" + rddType + "/" + operation;
    }
}
