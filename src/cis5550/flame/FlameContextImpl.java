package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.tools.*;

import java.util.List;

public class FlameContextImpl implements cis5550.flame.FlameContext {
    private final StringBuilder output = new StringBuilder();
    //private final KVSClient kvsClient = new KVSClient();
//    private String inputTable;

    private final KVSClient kvsClient;
    private final Logger logger = Logger.getLogger(FlameContextImpl.class);

    public FlameContextImpl(KVSClient client) {
        this.kvsClient = client;
    }

    @Override
    public KVSClient getKVS() {
        return null;
    }

    @Override
    public void output(String s) {
        output.append(s);
    }

    @Override
    public FlameRDD parallelize(List<String> list) throws Exception {
        logger.debug("Parallelize called " + list.size());
        String tableName = TableNameGenerator.generateUniqueTableName("parallelize");
//        this.inputTable = tableName;
        int seq = 0;
        for(String s : list) {
            String row = Hasher.hash("" + seq++);
            String column = "value";
            kvsClient.put(tableName, row, column, s);
        }
        return new FlameRDDImpl(this.kvsClient, tableName);
    }

    @Override
    public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
        RddAgent rddAgent = new RddAgent(this.kvsClient, tableName);
        String outputTable = rddAgent.invokeOperation("fromTable", Serializer.objectToByteArray(lambda), "rdd");
        return new FlameRDDImpl(this.kvsClient, outputTable);
    }

    @Override
    public void setConcurrencyLevel(int keyRangesPerWorker) {
        RddAgent.setConcurrencyLevel(keyRangesPerWorker);
    }

    public String getOutput() {
        return output.toString();
    }
}
