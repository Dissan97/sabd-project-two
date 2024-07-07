package dissanuddinahmed.queries;

import dissanuddinahmed.queries.functions.GroupFailures;
import dissanuddinahmed.queries.functions.KeyCountFailures;
import dissanuddinahmed.queries.functions.MakeRankingLined;
import dissanuddinahmed.utils.ProjectUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
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

        /*
        [0]: date
        [1]: serial_number
        [2]: model
        [3]: failure
        [4]: vault_id
         */

        DataStream<Tuple5<Long, String, String, Boolean, Integer>> vaultFailures = source.flatMap(
                        (FlatMapFunction<String, Tuple5<Long, String, String, Boolean, Integer>>) (value, out) -> {
                            String[] records = value.split("\\n");
                            for (String record : records) {
                                String[] values = record.split(",", -1);
                                if (values.length == 39) {
                                    try {
                                        long timestamp = Long.parseLong(values[0]);
                                        int vaultId = Integer.parseInt(values[4]);
                                        boolean failure = Integer.parseInt(values[3]) == 1;
                                        out.collect(new Tuple5<>(
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
                ).returns(Types.TUPLE(Types.LONG, Types.STRING, Types.STRING, Types.BOOLEAN, Types.INT))
                .filter(t -> t.f3)
                .assignTimestampsAndWatermarks(ProjectUtils.getWatermarkStrategy());

        Map<Integer, FileSink<String/*Tuple4<Long, Integer, Long, String>>*/>> sinkMap = new HashMap<>();
        for (int days : ProjectUtils.windowsDays) {
            OutputFileConfig config = OutputFileConfig
                    .builder()
                    .withPartPrefix("Q2-W-"+days)
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

            DataStream<Tuple4<Long, Integer, Long, String>> topFailures = vaultFailures.keyBy(
                            t -> t.f4)
                    .window(TumblingEventTimeWindows.of(Duration.ofDays(days)))
                    .process(new KeyCountFailures()).
                    returns(Types.TUPLE(Types.LONG, Types.INT, Types.LONG, Types.STRING))
                    .windowAll(
                            TumblingEventTimeWindows.of(Duration.ofDays(days))
                    ).apply(new GroupFailures());


            topFailures.windowAll(
                    TumblingEventTimeWindows.of(Duration.ofDays(days))
            ).apply(new MakeRankingLined()).returns(Types.STRING).map(//needed for output constrain
                            t -> t.replaceAll("\\{","([")
                                    .replaceAll("=\\[", ", ")
                                    .replaceAll("]}",")]")
                    ).sinkTo(sinkMap.get(days));
            //topFailures.sinkTo(sinkMap.get(days));
        }

    }
}
