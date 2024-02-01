package cis5550.test;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;

import java.util.Arrays;
import java.util.List;

public class FlameIntersection {
    public static void run(FlameContext ctx, String[] args) throws Exception {
        List<String> list = Arrays.asList(args);
        FlameRDD rdd1 = ctx.parallelize(list.subList(0, list.size() / 2));
        FlameRDD rdd2 = ctx.parallelize(list.subList(list.size() / 2, list.size()));
        List<String> out = rdd1.intersection(rdd2).collect();
        String result = "";
        for (String s : out)
            result = result + (result.equals("") ? "" : ",") + s;
        ctx.output(result);
    }
}
