package dissanuddinahmed.queries.functions.second;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KeyCountFailures extends
        ProcessWindowFunction<Tuple5<Long, String, String, Boolean, Integer>,
                Tuple4<Long, Integer, Long, String>, Integer, TimeWindow> {

    /**
     * Grouping the window and counting events
     * @param key vault_od
     * @param tuples allTuplesFor key
     * @param out data
     */
    @Override
    public void process(Integer key, ProcessWindowFunction<Tuple5<Long, String, String, Boolean, Integer>,
            Tuple4<Long, Integer, Long, String>, Integer,
            TimeWindow>.Context context,
                        Iterable<Tuple5<Long, String, String, Boolean, Integer>> tuples,
                        Collector<Tuple4<Long, Integer, Long, String>> out) {

        int vaultId = key;
        long count = 0L;
        Map<String, List<String>> modelSerialNumberMap = new HashMap<>();

        for (Tuple5<Long, String, String, Boolean, Integer> tuple : tuples) {
            if (modelSerialNumberMap.get(tuple.f2) == null){
                List<String> serialNumbers = new ArrayList<>();
                serialNumbers.add(tuple.f1);
                modelSerialNumberMap.put(tuple.f2, serialNumbers);
            }else {
                modelSerialNumberMap.get(tuple.f2).add(tuple.f1);
            }
            count++;
        }

        long timestamp = context.window().getEnd();

        out.collect(new Tuple4<>(
                timestamp,
                vaultId,
                count,
                modelSerialNumberMap.toString()

        ));
    }
}
