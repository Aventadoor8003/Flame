package cis5550.test;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;

import java.util.List;

public class FlameSample {
    public static void run(FlameContext ctx, String[] args) throws Exception {
        List<String> list = List.of(args);
        FlameRDD rdd = ctx.parallelize(list);
        List<String> out = rdd.sample(0.5).collect();
        String result = "";
        for (String s : out)
            result = result + (result.equals("") ? "" : ",") + s;
        ctx.output(result);
    }
}
