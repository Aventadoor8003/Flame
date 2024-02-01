package cis5550.flame;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.net.*;
import java.io.*;
import java.util.concurrent.atomic.AtomicInteger;

import static cis5550.webserver.Server.*;

import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.Serializer;
import cis5550.kvs.*;
import cis5550.tools.TableNameGenerator;
import cis5550.webserver.Request;

class Worker extends cis5550.generic.Worker {
    private static final Logger logger = Logger.getLogger(Worker.class);

    public static void main(String args[]) {
        if (args.length != 2) {
            System.err.println("Syntax: Worker <port> <coordinatorIP:port>");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        String server = args[1];
        startPingThread(server, "" + port, port);
        final File myJAR = new File("__worker" + port + "-current.jar");

        port(port);

        post("/useJAR", (request, response) -> {
            FileOutputStream fos = new FileOutputStream(myJAR);
            fos.write(request.bodyAsBytes());
            fos.close();
            return "OK";
        });


        post("/rdd/flatMap", (request, response) -> {
            String outputTableName = request.queryParams("outputTable");
            String kvsAddr = request.queryParams("kvs");
            String kvsStartKey = request.queryParams("fromKey");
            String kvsEndKeyEx = request.queryParams("toKey");
            String inputTable = request.queryParams("inputTable");
            FlameRDD.StringToIterable lambda = (FlameRDD.StringToIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);

//            Random rand = new Random();
            KVSClient kvsClient = new KVSClient(kvsAddr);
            Iterator<Row> iter = kvsClient.scan(inputTable, kvsStartKey, kvsEndKeyEx);
            while(iter.hasNext()) {
                Row row = iter.next();
                if(row == null) {
                    continue;
                }
                String input = row.get("value");
                Iterable<String> outputIter = lambda.op(input);
                for(String output : outputIter) {
                    String outputKey = Hasher.hash(output + UUID.randomUUID());
                    kvsClient.put(outputTableName, outputKey, "value", output);
                }
            }
            return null;
        });

        post("/rdd/mapToPair", (request, response) -> {
            String outputTableName = request.queryParams("outputTable");
            String kvsAddr = request.queryParams("kvs");
            String kvsStartKey = request.queryParams("fromKey");
            String kvsEndKeyEx = request.queryParams("toKey");
            String inputTable = request.queryParams("inputTable");
            FlameRDD.StringToPair lambda = (FlameRDD.StringToPair) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);

            KVSClient kvsClient = new KVSClient(kvsAddr);
            Iterator<Row> iter = kvsClient.scan(inputTable, kvsStartKey, kvsEndKeyEx);
            while(iter.hasNext()) {
                Row row = iter.next();
                if(row == null) {
                    continue;
                }
                String inputKey = row.key();
                String input = row.get("value");
                FlamePair output = lambda.op(input);
                kvsClient.put(outputTableName, output._1(), inputKey, output._2());
            }
            return null;
        });

        post("/pairRdd/foldByKey", (request, response) -> {
            String outputTableName = request.queryParams("outputTable");
            String kvsAddr = request.queryParams("kvs");
            String kvsStartKey = request.queryParams("fromKey");
            String kvsEndKeyEx = request.queryParams("toKey");
            String inputTable = request.queryParams("inputTable");
            String zeroElement = URLDecoder.decode(request.queryParams("zeroElement"), StandardCharsets.UTF_8);
            FlamePairRDD.TwoStringsToString lambda = (FlamePairRDD.TwoStringsToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);

            logger.debug("[flame worker] Starting foldByKey handling. " + lambda + " " + outputTableName + " " + kvsAddr + " " + kvsStartKey + " " + kvsEndKeyEx + " " + inputTable);
            KVSClient kvsClient = new KVSClient(kvsAddr);
            Iterator<Row> iter = kvsClient.scan(inputTable, kvsStartKey, kvsEndKeyEx);
            if(!iter.hasNext()) {
                logger.debug("[flame worker] No input rows");
            }
            while(iter.hasNext()) {
                String accumulator = zeroElement;
                Row row = iter.next();
                if(row == null) {
                    continue;
                }
                Set<String> columns = row.columns();
                for(String column : columns) {
                    String input = row.get(column);
                    accumulator = lambda.op(accumulator, input);
                }
                kvsClient.put(outputTableName, row.key(), "value", accumulator);
            }
            return null;
        });

        post("/rdd/distinct", (request, response) -> {
            String outputTableName = request.queryParams("outputTable");
            String kvsAddr = request.queryParams("kvs");
            String kvsStartKey = request.queryParams("fromKey");
            String kvsEndKeyEx = request.queryParams("toKey");
            String inputTable = request.queryParams("inputTable");

            KVSClient kvsClient = new KVSClient(kvsAddr);
            Iterator<Row> iter = kvsClient.scan(inputTable, kvsStartKey, kvsEndKeyEx);
            while(iter.hasNext()) {
                Row row = iter.next();
                if(row == null) {
                    continue;
                }
                String input = row.get("value");
                kvsClient.put(outputTableName, input, "value", input);
            }
            return null;
        });

        post("/rdd/sample", (request, response) -> {
            String outputTableName = request.queryParams("outputTable");
            String kvsAddr = request.queryParams("kvs");
            String kvsStartKey = request.queryParams("fromKey");
            String kvsEndKeyEx = request.queryParams("toKey");
            String inputTable = request.queryParams("inputTable");
            double prob = Double.parseDouble(request.queryParams("prob"));

            KVSClient kvsClient = new KVSClient(kvsAddr);
            Iterator<Row> iter = kvsClient.scan(inputTable, kvsStartKey, kvsEndKeyEx);
            while(iter.hasNext()) {
                Row row = iter.next();
                if(row == null) {
                    continue;
                }
                if(Math.random() < prob) {
                    kvsClient.put(outputTableName, row.key(), "value", row.get("value"));
                }
            }
            return null;
        });

        post("/rdd/intersection", (request, response) -> {
            String outputTableName = request.queryParams("outputTable");
            String kvsAddr = request.queryParams("kvs");
            String kvsStartKey = request.queryParams("fromKey");
            String kvsEndKeyEx = request.queryParams("toKey");
            String inputTable = request.queryParams("inputTable");
            String inputTable2 = request.queryParams("inputTable2");

            logger.debug("[flame worker] Starting intersection handling. " + outputTableName + " " + kvsAddr + " " + kvsStartKey + " " + kvsEndKeyEx + " " + inputTable + " " + inputTable2);

            Set<String> set1 = new HashSet<>();
            Set<String> set2 = new HashSet<>();
            KVSClient kvsClient = new KVSClient(kvsAddr);
            Iterator<Row> iter = kvsClient.scan(inputTable, kvsStartKey, kvsEndKeyEx);
            while(iter.hasNext()) {
                Row row = iter.next();
                if(row == null) {
                    continue;
                }
                String input = row.get("value");
                set1.add(input);
            }
            iter = kvsClient.scan(inputTable2, kvsStartKey, kvsEndKeyEx);
            while(iter.hasNext()) {
                Row row = iter.next();
                if(row == null) {
                    continue;
                }
                String input = row.get("value");
                set2.add(input);
            }
            set1.retainAll(set2);
            for(String input : set1) {
                kvsClient.put(outputTableName, Hasher.hash(input + UUID.randomUUID()), "value", input);
            }
            return null;
        });

        post("/rdd/fromTable", (request, response) -> {
            String outputTableName = request.queryParams("outputTable");
            String kvsAddr = request.queryParams("kvs");
            String kvsStartKey = request.queryParams("fromKey");
            String kvsEndKeyEx = request.queryParams("toKey");
            String inputTable = request.queryParams("inputTable");
            FlameContext.RowToString lambda = (FlameContext.RowToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);

            KVSClient kvsClient = new KVSClient(kvsAddr);
            Iterator<Row> iter = kvsClient.scan(inputTable, kvsStartKey, kvsEndKeyEx);
            while(iter.hasNext()) {
                Row row = iter.next();
                if(row == null) {
                    continue;
                }
                String output = lambda.op(row);
                if(output != null) {
                    kvsClient.put(outputTableName, row.key(), "value", output);
                }
            }
            return null;
        });

        post("/pairRdd/flatMap", (request, response) -> {
            String outputTableName = request.queryParams("outputTable");
            String kvsAddr = request.queryParams("kvs");
            String kvsStartKey = request.queryParams("fromKey");
            String kvsEndKeyEx = request.queryParams("toKey");
            String inputTable = request.queryParams("inputTable");
            FlamePairRDD.PairToStringIterable lambda = (FlamePairRDD.PairToStringIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);

            KVSClient kvsClient = new KVSClient(kvsAddr);
            Iterator<Row> iter = kvsClient.scan(inputTable, kvsStartKey, kvsEndKeyEx);
            while(iter.hasNext()) {
                Row row = iter.next();
                if(row == null) {
                    continue;
                }
                Set<String> columns = row.columns();
                for(String column : columns) {
                    FlamePair pair = new FlamePair(row.key(), row.get(column));
                    Iterable<String> iterable = lambda.op(pair);
                    for(String output : iterable) {
                        String outputKey = Hasher.hash(output + UUID.randomUUID());
                        kvsClient.put(outputTableName, outputKey, "value", output);
                    }
                }
            }
            return null;
        });

        post("/pairRdd/flatMapToPair", (request, response) -> {
            String outputTableName = request.queryParams("outputTable");
            String kvsAddr = request.queryParams("kvs");
            String kvsStartKey = request.queryParams("fromKey");
            String kvsEndKeyEx = request.queryParams("toKey");
            String inputTable = request.queryParams("inputTable");
            FlamePairRDD.PairToPairIterable lambda = (FlamePairRDD.PairToPairIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);

            KVSClient kvsClient = new KVSClient(kvsAddr);
            Iterator<Row> iter = kvsClient.scan(inputTable, kvsStartKey, kvsEndKeyEx);
            while(iter.hasNext()) {
                Row row = iter.next();
                if(row == null) {
                    continue;
                }
                Set<String> columns = row.columns();
                for(String column : columns) {
                    FlamePair pair = new FlamePair(row.key(), row.get(column));
                    Iterable<FlamePair> iterable = lambda.op(pair);
                    for(FlamePair output : iterable) {
                        kvsClient.put(outputTableName, output._1(), row.key(), output._2());
                    }
                }
            }
            return null;
        });

        post("/rdd/flatMapToPair", (request, response) -> {
            String outputTableName = request.queryParams("outputTable");
            String kvsAddr = request.queryParams("kvs");
            String kvsStartKey = request.queryParams("fromKey");
            String kvsEndKeyEx = request.queryParams("toKey");
            String inputTable = request.queryParams("inputTable");
            FlameRDD.StringToPairIterable lambda = (FlameRDD.StringToPairIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);

            KVSClient kvsClient = new KVSClient(kvsAddr);
            Iterator<Row> iter = kvsClient.scan(inputTable, kvsStartKey, kvsEndKeyEx);
            while(iter.hasNext()) {
                Row row = iter.next();
                if(row == null) {
                    continue;
                }
                String input = row.get("value");
                Iterable<FlamePair> iterable = lambda.op(input);
                for(FlamePair output : iterable) {
                    kvsClient.put(outputTableName, output._1(), row.key(), output._2());
                }
            }
            return null;
        });

        post("/pairRdd/join", (request, response) -> {
            String outputTableName = request.queryParams("outputTable");
            String kvsAddr = request.queryParams("kvs");
            String kvsStartKey = request.queryParams("fromKey");
            String kvsEndKeyEx = request.queryParams("toKey");
            String inputTable = request.queryParams("inputTable");
            String otherTable = request.queryParams("otherTable");

            KVSClient kvsClient = new KVSClient(kvsAddr);
            Iterator<Row> iter = kvsClient.scan(inputTable, kvsStartKey, kvsEndKeyEx);
            while (iter.hasNext()) {
                Row row = iter.next();
                if (row == null) {
                    continue;
                }
                Set<String> columns = row.columns();
                for (String column : columns) {
                    String input = row.get(column);
                    Row otherRow = kvsClient.getRow(otherTable, row.key());
                    if (otherRow == null) {
                        continue;
                    }
//                    logger.debug("[flame worker] on key" + row.key() + " Joining " + input + " with " + otherRow.get("value"));
                    Set<String> otherColumns = otherRow.columns();
                    for (String otherColumn : otherColumns) {
                        String otherInput = otherRow.get(otherColumn);
                        kvsClient.put(outputTableName, row.key(), UUID.randomUUID().toString(), input + "," + otherInput);
                    }
                }
            }
            return null;
        });

        post("/rdd/fold", (request, response) -> {
            String outputTableName = request.queryParams("outputTable");
            String kvsAddr = request.queryParams("kvs");
            String kvsStartKey = request.queryParams("fromKey");
            String kvsEndKeyEx = request.queryParams("toKey");
            String inputTable = request.queryParams("inputTable");
            String zeroElement = URLDecoder.decode(request.queryParams("zeroElement"), StandardCharsets.UTF_8);
            FlamePairRDD.TwoStringsToString lambda = (FlamePairRDD.TwoStringsToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);

            KVSClient kvsClient = new KVSClient(kvsAddr);
            Iterator<Row> iter = kvsClient.scan(inputTable, kvsStartKey, kvsEndKeyEx);
            String accumulator = zeroElement;
            while(iter.hasNext()) {
                Row row = iter.next();
                if(row == null) {
                    continue;
                }
                String input = row.get("value");
                accumulator = lambda.op(accumulator, input);
            }
            kvsClient.put(outputTableName, UUID.randomUUID().toString(), "value", accumulator);
            return null;
        });

        post("/rdd/filter", (request, response) -> {
            String outputTableName = request.queryParams("outputTable");
            String kvsAddr = request.queryParams("kvs");
            String kvsStartKey = request.queryParams("fromKey");
            String kvsEndKeyEx = request.queryParams("toKey");
            String inputTable = request.queryParams("inputTable");
            FlameRDD.StringToBoolean lambda = (FlameRDD.StringToBoolean) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);

            KVSClient kvsClient = new KVSClient(kvsAddr);
            Iterator<Row> iter = kvsClient.scan(inputTable, kvsStartKey, kvsEndKeyEx);
            while (iter.hasNext()) {
                Row row = iter.next();
                if (row == null) {
                    continue;
                }
                String input = row.get("value");
                if (lambda.op(input)) {
                    kvsClient.put(outputTableName, row.key(), "value", input);
                }
            }
            return null;
        } );

        post("/rdd/mapPartitions", (request, response) -> {
            String outputTableName = request.queryParams("outputTable");
            String kvsAddr = request.queryParams("kvs");
            String kvsStartKey = request.queryParams("fromKey");
            String kvsEndKeyEx = request.queryParams("toKey");
            String inputTable = request.queryParams("inputTable");
            FlameRDD.IteratorToIterator lambda = (FlameRDD.IteratorToIterator) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);

            KVSClient kvsClient = new KVSClient(kvsAddr);
            Iterator<Row> iter = kvsClient.scan(inputTable, kvsStartKey, kvsEndKeyEx);
            List<String> inputList = new ArrayList<>();
            while (iter.hasNext()) {
                Row row = iter.next();
                if (row == null) {
                    continue;
                }
                String input = row.get("value");
                inputList.add(input);
            }
            Iterator<String> inputIter = inputList.iterator();
            Iterator<String> outputIter = lambda.op(inputIter);
            while (outputIter.hasNext()) {
                String output = outputIter.next();
                kvsClient.put(outputTableName, Hasher.hash(output + UUID.randomUUID()), "value", output);
            }
            return null;
        });

        post("/pairRdd/cogroup", (request, response) -> {
            String outputTableName = request.queryParams("outputTable");
            String kvsAddr = request.queryParams("kvs");
            String kvsStartKey = request.queryParams("fromKey");
            String kvsEndKeyEx = request.queryParams("toKey");
            String inputTable = request.queryParams("inputTable");
            String otherTable = request.queryParams("otherTable");

            KVSClient kvsClient = new KVSClient(kvsAddr);
            Iterator<Row> iter = kvsClient.scan(inputTable, kvsStartKey, kvsEndKeyEx);
            Set<String> visited = new HashSet<>();
            while (iter.hasNext()) {
                Row row = iter.next();
                if (row == null) {
                    continue;
                }
                Box box = new Box();
                Box otherBox = new Box();
                String key = row.key();
                logger.debug("[flame worker] round 1 on key" + key);
                visited.add(key);
                Row otherRow = kvsClient.getRow(otherTable, key);
                Set<String> columns = row.columns();
                for (String column : columns) {
                    String input = row.get(column);
                    box.append(input);
                }

                Set<String> otherColumns = otherRow == null ? new HashSet<>() : otherRow.columns();
                for (String otherColumn : otherColumns) {
                    String otherInput = otherRow.get(otherColumn);
                    otherBox.append(otherInput);
                }
                kvsClient.put(outputTableName, key, "value", box + "," + otherBox);
            }

            iter = kvsClient.scan(otherTable, kvsStartKey, kvsEndKeyEx);
            while (iter.hasNext()) {
                Row row = iter.next();
                if (row == null || visited.contains(row.key())) {
                    continue;
                }
                logger.debug("[flame worker] round 2 on key" + row.key());
                Box box = new Box();
                Set<String> columns = row.columns();
                for (String column : columns) {
                    String input = row.get(column);
                    box.append(input);
                }
                kvsClient.put(outputTableName, row.key(), "value", "[]," + box);
            }

            return null;
        });

    }


}

class Box {
    private final List<String> elements;

    public Box() {
        elements = new ArrayList<>();
    }

    public void append(String element) {
        elements.add(element);
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        for(String element : elements) {
            builder.append(element).append(",");
        }
        if(!elements.isEmpty()) {
            builder.deleteCharAt(builder.length() - 1);
        }
        builder.append("]");
        return builder.toString();
    }
}
