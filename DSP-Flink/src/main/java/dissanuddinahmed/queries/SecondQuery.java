package dissanuddinahmed.queries;

import dissanuddinahmed.queries.functions.ThroughputMetricsMapper;
import dissanuddinahmed.queries.functions.second.GroupFailures;
import dissanuddinahmed.queries.functions.second.KeyCountFailures;
import dissanuddinahmed.queries.functions.second.MakeRankingLined;
import dissanuddinahmed.queries.functions.second.QueryTwoFlatMap;
import dissanuddinahmed.utils.ProjectUtils;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
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

public class SecondQuery {

    public static void launch(DataStream<String> source) {

        DataStream<Tuple5<Long, String, String, Boolean, Integer>> vaultFailures = source.flatMap(
                new QueryTwoFlatMap()
            ).setParallelism(ProjectUtils.PARALLELISM.get())
            .returns(Types.TUPLE(Types.LONG, Types.STRING, Types.STRING, Types.BOOLEAN, Types.INT))
            .filter(t -> t.f3).name("Q2_filter")
            .setParallelism(ProjectUtils.PARALLELISM.get())
            //.map(new LatencyMetricsMapper<>())
            //.map(new ThroughputMetricsMapper<>()).name("Q2_before_watermark")
            .assignTimestampsAndWatermarks(ProjectUtils.getWatermarkStrategy());

        Map<Integer, FileSink<String/*Tuple4<Long, Integer, Long, String>>*/>> sinkMap = new HashMap<>();
        for (int days : ProjectUtils.windowsDays) {
            OutputFileConfig config = OutputFileConfig
                .builder()
                .withPartPrefix("Q2-W-" + days)
                .withPartSuffix(".csv")
                .build();
            sinkMap.put(days, FileSink
                .forRowFormat(new Path("file:///results/queries/query2"),
                    new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(config)
                .build());
        }

        for (int days : ProjectUtils.windowsDays) {
            long offset = 0;

            if (days == 23){
                offset = 13;
            }
            DataStream<Tuple4<Long, Integer, Long, String>> topFailures = vaultFailures.keyBy(
                    t -> t.f4)
                .window(TumblingEventTimeWindows.of(Duration.ofDays(days), Duration.ofDays(offset)))
                .process(new KeyCountFailures()).name("Q2_process")
                .setParallelism(ProjectUtils.PARALLELISM.get())
                .returns(Types.TUPLE(Types.LONG, Types.INT, Types.LONG, Types.STRING))
                .map(new ThroughputMetricsMapper<>()).name("Q2_watermark_1_d"+days)
                .windowAll(
                    TumblingEventTimeWindows.of(Duration.ofDays(days), Duration.ofDays(offset))
                ).apply(new GroupFailures()).name("Q2_rank_apply")
                .map(new ThroughputMetricsMapper<>()).name("Q2_Watermark_2_d"+days);

            topFailures.windowAll(
                TumblingEventTimeWindows.of(Duration.ofDays(days))
            ).apply(new MakeRankingLined()).returns(Types.STRING).map(//needed for output constrain
                t -> t.replaceAll("\\{", "([")
                    .replaceAll("=\\[", ", ")
                    .replaceAll("]}", "])")
            ).name("Q2_sink_W_"+days).sinkTo(sinkMap.get(days));
        }

    }
}
