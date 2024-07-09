package dissanuddinahmed.queries.functions.first;

import com.google.common.util.concurrent.AtomicDouble;
import dissanuddinahmed.utils.ProjectUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class QueryOneFlatMap implements FlatMapFunction<String, Tuple3<Long, Integer, Double>> {

    //This variable is used to handle null temperature values
    private final AtomicDouble atomicTemperature = new AtomicDouble(0);


    @Override
    public void flatMap(String input, Collector<Tuple3<Long, Integer, Double>> output) {
        String[] records = input.split("\\n");
        for (String record : records) {
            String[] values = record.split(",", -1);
            if (values.length >= 38) {
                long timestamp = Long.parseLong(values[0]);
                int vaultId = Integer.parseInt(values[4]);
                double temperature;
                try {
                    temperature = Double.parseDouble(values[25]);
                    double old = atomicTemperature.get();
                    atomicTemperature.set(Double.parseDouble(
                        ProjectUtils.DECIMAL_FORMAT.format((temperature + old) / 2.0)));
                } catch (NumberFormatException ignored) {
                    temperature = atomicTemperature.get();
                }
                output.collect(new Tuple3<>(timestamp, vaultId, temperature));
            }
        }
    }
}
