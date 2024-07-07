package dissanuddinahmed.queries.functions;

import dissanuddinahmed.utils.ProjectUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TemperatureStatsMetrics extends
        ProcessWindowFunction<Tuple3<Long, Integer, Double>, Tuple5<Long,
                Integer, Integer, Double, Double>, Integer, TimeWindow> {

    private transient Counter elementsProcessedCounter;
    private transient Meter elementsProcessedMeter;
    private transient Histogram processingTimeHistogram;

    @Override
    public void open(Configuration parameters) {
        elementsProcessedCounter = getRuntimeContext()
                .getMetricGroup()
                .counter("Q1_TemperatureProcessCounter");
        elementsProcessedMeter = getRuntimeContext()
                .getMetricGroup()
                .meter("Q1_TemperatureProcessMeter", new MeterView(1));
        processingTimeHistogram = getRuntimeContext()
                .getMetricGroup()
                .histogram("Q1_ProcessingTimeHistogram", new DescriptiveStatisticsHistogram(1000));
    }

    @Override
    public void process(Integer key, Context context, Iterable<Tuple3<Long, Integer, Double>> elements,
                        Collector<Tuple5<Long, Integer, Integer, Double, Double>> out) {
        long startTime = System.currentTimeMillis();

        int vaultId = key;
        int count = 0;
        double mean = 0.0;
        double m2 = 0.0;

        for (Tuple3<Long, Integer, Double> element : elements) {
            double temperature = element.f2;

            count++;

            double delta = temperature - mean;
            mean += delta / count;
            double delta2 = temperature - mean;
            m2 += delta * delta2;
        }

        double variance = count > 1 ? m2 / (count - 1) : 0.0;
        double standardDeviation = Math.sqrt(variance);

        long timestamp = context.window().getEnd();

        out.collect(new Tuple5<>(timestamp, vaultId, count,
                Double.parseDouble(ProjectUtils.DECIMAL_FORMAT.format(mean)),
                Double.parseDouble(ProjectUtils.DECIMAL_FORMAT.format(standardDeviation))));

        long processingTime = System.currentTimeMillis() - startTime;
        elementsProcessedCounter.inc(count);
        elementsProcessedMeter.markEvent(count);
        processingTimeHistogram.update(processingTime);
    }
}
