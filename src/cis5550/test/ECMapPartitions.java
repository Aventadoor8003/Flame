package cis5550.test;

import cis5550.flame.FlameRDD;

import java.util.Arrays;
import java.util.Iterator;

public class ECMapPartitions {
    public static void run(cis5550.flame.FlameContext ctx, String args[]) throws Exception {
        FlameRDD data = ctx.parallelize(Arrays.asList("Apple", "Banana", "Coconut", "Date"));
        FlameRDD out = data.mapPartitions(iter -> {
            final String suffix = " is yummy"; /* Initialization */
            return new Iterator<>() {
                public boolean hasNext() { return iter.hasNext(); }
                public String next() { return iter.next() + suffix; }
            };
        });
        ctx.output(out.collect().toString());
    }
}
