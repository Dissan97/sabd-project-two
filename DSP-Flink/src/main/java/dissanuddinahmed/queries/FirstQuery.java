package dissanuddinahmed.queries;

import dissanuddinahmed.queries.functions.ThroughputMetricsMapper;
import dissanuddinahmed.queries.functions.first.QueryOneFilterVault;
import dissanuddinahmed.queries.functions.first.QueryOneFlatMap;
import dissanuddinahmed.queries.functions.first.TemperatureStatsMetrics;
import dissanuddinahmed.utils.ProjectUtils;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class FirstQuery {
    private FirstQuery() {
    }

    private static final String HEADERS = "ts,vaultid,count,means194,stddevs194";

    public static void launch(DataStream<String> source) {


        DataStream<Tuple3<Long, Integer, Double>> temperatureValues = source.flatMap(
                new QueryOneFlatMap()
            ).setParallelism(ProjectUtils.PARALLELISM.get())
            .name("Q1_flatmap").returns(Types.TUPLE(Types.LONG, Types.INT, Types.DOUBLE))
            .assignTimestampsAndWatermarks(ProjectUtils.getWatermarkStrategy());

        Map<Integer, FileSink<String>> sinkMap = new HashMap<>();
        //needed to add header
        Map<Integer, AtomicBoolean> headerMap = new HashMap<>();
        for (int days : ProjectUtils.windowsDays) {
            OutputFileConfig config = OutputFileConfig
                .builder()
                .withPartPrefix("Q1-W-" + days)
                .withPartSuffix(".csv")
                .build();
            sinkMap.put(days, FileSink
                .forRowFormat(new Path("file:///results/queries/query1"),
                    new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(config)
                .build());
            headerMap.put(days, new AtomicBoolean(true));
        }

        DataStream<Tuple3<Long, Integer, Double>> filteredVault = temperatureValues
            .filter(new QueryOneFilterVault())
            .setParallelism(ProjectUtils.PARALLELISM.get());
            //.map(new LatencyMetricsMapper<>())
            //.map(new ThroughputMetricsMapper<>()).name("Q_1_before_watermark");

        for (int days : ProjectUtils.windowsDays) {
            long offset = 0;

            if (days == 23){
                offset = 13;
            }
            if (days == 3){
                offset = 2;
            }
            AtomicBoolean needHeader = headerMap.get(days);
            DataStream<Tuple5<Long, Integer, Integer, Double, Double>> results = filteredVault
                .keyBy(t -> t.f1)
                .window(
                    TumblingEventTimeWindows.of(Duration.ofDays(days), Duration.ofDays(offset)))
                .process(
                    new TemperatureStatsMetrics()).name("Q1_process")
                .returns(Types.TUPLE(Types.LONG, Types.INT, Types.INT, Types.DOUBLE, Types.DOUBLE))
                .map(new ThroughputMetricsMapper<>()).name("Q1_watermark_d_"+days);

            //to put header and data to the output file removing the brackets
            results.map(
                t -> {
                    String ret = t.toString().substring(1, t.toString().length() - 1);
                    if (needHeader.get()) {
                        needHeader.set(false);
                        return HEADERS + "\n" +
                            ret;
                    }
                    return ret;
                }
            ).name("Q1_Sink_W_" + days).sinkTo(sinkMap.get(days));
            //results.print();
        }
    }
}