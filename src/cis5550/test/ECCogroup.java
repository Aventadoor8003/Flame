package cis5550.test;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;

import java.util.Arrays;

public class ECCogroup {
    public static void run(FlameContext ctx, String args[]) throws Exception {
        FlameRDD rdd1 = ctx.parallelize(Arrays.asList("Banana", "Date", "Coconut", "Apple"));
        FlameRDD rdd2 = ctx.parallelize(Arrays.asList("Cashew","Durian","Cherry","Avocado","Egg"));
        FlamePairRDD pairRDD1 = rdd1.mapToPair(s -> new FlamePair(s.substring(0, 1), s));
        FlamePairRDD pairRDD2 = rdd2.mapToPair(s -> new FlamePair(s.substring(0, 1), s));
        FlamePairRDD out = pairRDD1.cogroup(pairRDD2);
        ctx.output(out.collect().toString());
    }
}
