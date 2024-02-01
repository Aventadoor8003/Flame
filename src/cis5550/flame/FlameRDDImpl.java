package cis5550.flame;

import cis5550.exception.TableRenameException;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Logger;
import cis5550.tools.RddAgent;
import cis5550.tools.Serializer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

public class FlameRDDImpl implements FlameRDD {
    private final KVSClient client;
    private String tableName;
    private static final Logger logger = Logger.getLogger(FlameRDDImpl.class);
    public FlameRDDImpl(KVSClient client, String tableName) {
        this.client = client;
        this.tableName = tableName;
    }

    @Override
    public int count() throws Exception {
        return client.count(tableName);
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
    public void destroy() throws Exception {
        client.delete(tableName);
    }

    @Override
    public Vector<String> take(int num) throws Exception {
        Vector<String> result = new Vector<>();
        Iterator<Row> iterator = client.scan(tableName);
        int count = 0;
        while (iterator.hasNext() && count < num) {
            Row row = iterator.next();
            result.add(row.get("value"));
            count++;
        }
        return result;
    }

    @Override
    public String fold(String zeroElement, FlamePairRDD.TwoStringsToString lambda) throws Exception {
        RddAgent agent = new RddAgent(client, tableName).addAdditionalParam("zeroElement", zeroElement);
        String outputTable = agent.invokeOperation("fold", Serializer.objectToByteArray(lambda), "rdd");
        Iterator<Row> iterator = client.scan(outputTable);
        String accumulator = zeroElement;
        while (iterator.hasNext()) {
            Row row = iterator.next();
            accumulator = lambda.op(accumulator, row.get("value"));
        }
        return accumulator;
    }

    @Override
    public List<String> collect() throws Exception {
        List<String> result = new ArrayList<>();
        Iterator<Row> iterator = client.scan(tableName);
        while (iterator.hasNext()) {
            Row row = iterator.next();
            result.add(row.get("value"));
        }
        return result;
    }

    @Override
    public FlameRDD flatMap(StringToIterable lambda) throws Exception {
        RddAgent utils = new RddAgent(client, tableName);
        String outputTable = utils.invokeOperation("flatMap", Serializer.objectToByteArray(lambda), "rdd");
        return new FlameRDDImpl(client, outputTable);
    }

    @Override
    public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
        RddAgent agent = new RddAgent(client, tableName);
        String outputTable = agent.invokeOperation("flatMapToPair", Serializer.objectToByteArray(lambda), "rdd");
        return new FlamePairRDDImpl(client, outputTable);
    }

    @Override
    public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
        RddAgent utils = new RddAgent(client, tableName);
        String outputTable = utils.invokeOperation("mapToPair", Serializer.objectToByteArray(lambda), "rdd");
        return new FlamePairRDDImpl(client, outputTable);
    }

    @Override
    public FlameRDD intersection(FlameRDD r) throws Exception {
        RddAgent agent = new RddAgent(client, tableName).addAdditionalParam("inputTable2", ((FlameRDDImpl)r).getTableName());
        logger.debug("Agent: " + agent);
        String outputTable = agent.invokeOperation("intersection", null, "rdd");
        return new FlameRDDImpl(client, outputTable);
    }

    @Override
    public FlameRDD sample(double f) throws Exception {
        RddAgent agent = new RddAgent(client, tableName).addAdditionalParam("prob", f + "");
        String outputTable = agent.invokeOperation("sample", null, "rdd");
        return new FlameRDDImpl(client, outputTable);
    }

    @Override
    public FlamePairRDD groupBy(StringToString lambda) throws Exception {
        return null;
    }

    @Override
    public FlameRDD filter(StringToBoolean lambda) throws Exception {
        RddAgent agent = new RddAgent(client, tableName);
        String outputTable = agent.invokeOperation("filter", Serializer.objectToByteArray(lambda), "rdd");
        return new FlameRDDImpl(client, outputTable);
    }

    @Override
    public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
        RddAgent agent = new RddAgent(client, tableName);
        String outputTable = agent.invokeOperation("mapPartitions", Serializer.objectToByteArray(lambda), "rdd");
        return new FlameRDDImpl(client, outputTable);
    }

    public String getTableName() {
        return tableName;
    }

    public FlameRDD distinct() throws Exception {
        RddAgent utils = new RddAgent(client, tableName);
        String outputTable = utils.invokeOperation("distinct", null, "rdd");
        return new FlameRDDImpl(client, outputTable);
    }
}
