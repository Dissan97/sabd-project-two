package dissanuddinahmed.queries.functions.second;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

public class QueryTwoFlatMap implements FlatMapFunction<String, Tuple5<Long, String, String, Boolean, Integer>> {

/*
    [0]: date
    [1]: serial_number
    [2]: model
    [3]: failure
    [4]: vault_id
 */

    @Override
    public void flatMap(String input, Collector<Tuple5<Long, String, String, Boolean, Integer>> output) {
        String[] records = input.split("\\n");
        for (String record : records) {
            String[] values = record.split(",", -1);
            if (values.length == 39) {
                try {
                    long timestamp = Long.parseLong(values[0]);
                    int vaultId = Integer.parseInt(values[4]);
                    boolean failure = Integer.parseInt(values[3]) == 1;
                    output.collect(new Tuple5<>(
                        timestamp,
                        values[1],
                        values[2],
                        failure,
                        vaultId));
                } catch (NumberFormatException ignored) {
                }
            }
        }
    }
}
