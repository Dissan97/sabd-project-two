package dissanuddinahmed.queries.functions;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.metrics.Gauge;


public class ThroughputMetricsMapper<T> extends RichMapFunction<T, T> {
    private double startTime = 0.0;
    private long counter = 0L;
    private double throughput = 0.0;

    @Override
    public void open(OpenContext openContext) {
        this.startTime = System.nanoTime() / Math.pow(10, 9); // timestamp in seconds
        this.counter = 0;
        getRuntimeContext()
            .getMetricGroup()
            .gauge("Gauge_Throughput", (Gauge<Double>) () -> throughput * 100000);
    }

    @Override
    public T map(T value) {
        this.counter += 1;
        double nowTimestamp = System.nanoTime() / Math.pow(10, 9);
        double totalDiffTime = nowTimestamp - this.startTime;
        // Average throughput [tuple / s]
        this.throughput = this.counter / totalDiffTime;

        return value;
    }
}
