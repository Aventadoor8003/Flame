package cis5550.flame;

import cis5550.exception.TableRenameException;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Logger;
import cis5550.tools.RddAgent;
import cis5550.tools.Serializer;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class FlamePairRDDImpl implements FlamePairRDD{
    private final KVSClient client;
    private String tableName;
    private static final Logger logger = Logger.getLogger(FlamePairRDDImpl.class);
    public FlamePairRDDImpl(KVSClient client, String associatedTable) {
        this.client = client;
        this.tableName = associatedTable;
    }
    @Override
    public List<FlamePair> collect() throws Exception {
        List<FlamePair> result = new ArrayList<>();
        Iterator<Row> iterator = client.scan(tableName);
        while (iterator.hasNext()) {
            Row row = iterator.next();
            Set<String> columns = row.columns();
            for (String column : columns) {
                result.add(new FlamePair(row.key(), row.get(column)));
            }
        }
        return result;
    }

    @Override
    public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
        RddAgent utils = new RddAgent(client, tableName).addAdditionalParam("zeroElement", URLEncoder.encode(zeroElement, StandardCharsets.UTF_8));
        String outputTable = utils.invokeOperation("foldByKey", Serializer.objectToByteArray(lambda), "pairRdd");
        return new FlamePairRDDImpl(client, outputTable);
    }

    @Override
    public void saveAsTable(String tableNameArg) throws Exception {
        boolean success = client.rename(tableName, tableNameArg);
        if(!success) {
            logger.error("Failed to rename table " + tableName + " to " + tableNameArg);
            throw new TableRenameException("Failed to rename table " + tableName + " to " + tableNameArg);
        }
        this.tableName = tableNameArg;
    }

    @Override
    public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
        RddAgent agent = new RddAgent(client, tableName);
        String outputTable = agent.invokeOperation("flatMap", Serializer.objectToByteArray(lambda), "pairRdd");
        return new FlameRDDImpl(client, outputTable);
    }

    @Override
    public void destroy() throws Exception {
        client.delete(tableName);
    }

    @Override
    public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
        RddAgent agent = new RddAgent(client, tableName);
        String outputTable = agent.invokeOperation("flatMapToPair", Serializer.objectToByteArray(lambda), "pairRdd");
        return new FlamePairRDDImpl(client, outputTable);
    }

    @Override
    public FlamePairRDD join(FlamePairRDD other) throws Exception {
        //System.out.println("PairRdd join invoked");
        RddAgent agent = new RddAgent(client, tableName).addAdditionalParam("otherTable", ((FlamePairRDDImpl)other).getTableName());
        String outputTable = agent.invokeOperation("join", null, "pairRdd");
        return new FlamePairRDDImpl(client, outputTable);
    }

    @Override
    public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
        RddAgent agent = new RddAgent(client, tableName).addAdditionalParam("otherTable", ((FlamePairRDDImpl)other).getTableName());
        String outputTable = agent.invokeOperation("cogroup", null, "pairRdd");
        return new FlamePairRDDImpl(client, outputTable);
    }

    public String getTableName() {
        return tableName;
    }
}
