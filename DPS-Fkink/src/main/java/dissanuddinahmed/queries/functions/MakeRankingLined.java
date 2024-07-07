package dissanuddinahmed.queries.functions;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MakeRankingLined implements AllWindowFunction<Tuple4<Long, Integer, Long, String>, String, TimeWindow> {
    @Override
    public void apply(TimeWindow timeWindow, Iterable<Tuple4<Long, Integer, Long, String>> iterable,
                      Collector<String> collector) {
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (Tuple4<Long, Integer, Long, String> t : iterable){
            if (first){
                first = false;
                builder.append(t.f0);
            }
            builder.append(',');
            builder.append(t.f1).append(',')
                    .append(t.f2).append(',')
                    .append(t.f3);
        }
        builder.append('\n');
        collector.collect(builder.toString());
    }
}
