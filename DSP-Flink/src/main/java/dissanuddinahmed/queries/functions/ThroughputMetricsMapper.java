package dissanuddinahmed.queries.functions;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.metrics.Gauge;

import java.util.logging.Logger;


public class MetricsMapper<T> extends RichMapFunction<T, T> {
    private static final Logger LOGGER = Logger.getLogger(MetricsMapper.class.getSimpleName());
    private long startTime = 0L;
    private long counter = 0L;
    private double throughput = 0.0;

    @Override
    public void open(OpenContext openContext) {
        this.startTime = System.nanoTime();
        this.counter = 0;
        getRuntimeContext()
            .getMetricGroup()
            .gauge("Throughput", (Gauge<Double>) () -> throughput * 100000000);
    }

    @Override
    public T map(T value) {
        this.counter += 1;
        long nowTimestamp = System.nanoTime();
        double totalDiffTime = nowTimestamp - this.startTime;

        // Average throughput [tuple / s]
        this.throughput = this.counter / totalDiffTime;

        return value;
    }
}
