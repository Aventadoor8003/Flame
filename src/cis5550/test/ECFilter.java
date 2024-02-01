package cis5550.test;

import cis5550.flame.FlameRDD;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ECFilter {
    public static void run(cis5550.flame.FlameContext ctx, String args[]) throws Exception {
        List<String> out = Arrays.asList("apple", "banana", "coconut", "dog", "egg", "fat", "giggle", "hello", "iterator", "jag", "end", "emo", "end");
        Collections.addAll(out, args);
        FlameRDD rdd = ctx.parallelize(out);
        FlameRDD rdd2 = rdd.filter(s -> s.startsWith("e"));
        ctx.output(rdd2.collect().toString());
    }
}
