package dissanuddinahmed.queries;

import com.google.common.util.concurrent.AtomicDouble;
import dissanuddinahmed.queries.functions.TemperatureStatsMetrics;
import dissanuddinahmed.queries.functions.TemperatureStatsProcess;
import dissanuddinahmed.utils.ProjectUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class FirstQuery {
    private FirstQuery(){}

    private static final String HEADERS = "ts,vaultid,count,means194,stddevs194";
    public static void launch(DataStream<String> source){

        //This variable is used to handle null temperature values
        AtomicDouble atomicTemperature = new AtomicDouble(0);

    /*
        [0]: date
        [4]: vault_id
        [25]: s194_temperature_celsius
     */
        DataStream<Tuple3<Long, Integer, Double>> temperatureValues = source.flatMap(
                        new RichFlatMapFunction<String, Tuple3<Long, Integer, Double>>() {

                            private transient Meter recordsOutMeter;

                            @Override
                            public void open(Configuration parameters) {
                                recordsOutMeter = getRuntimeContext()
                                        .getMetricGroup()
                                        .meter("Q1_flatmap", new MeterView(1)); // 60 seconds rate
                            }

                            @Override
                            public void flatMap(String value, Collector<Tuple3<Long, Integer, Double>> out) {
                                String[] records = value.split("\\n");
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
                                        out.collect(new Tuple3<>(timestamp, vaultId, temperature));
                                        recordsOutMeter.markEvent();
                                    }
                                }
                            }
                        }
                ).returns(Types.TUPLE(Types.LONG, Types.INT, Types.DOUBLE))
                .assignTimestampsAndWatermarks(ProjectUtils.getWatermarkStrategy());

        Map<Integer, FileSink<String>> sinkMap = new HashMap<>();
        //needed to add header
        Map<Integer, AtomicBoolean> headerMap = new HashMap<>();
        for (int days: ProjectUtils.windowsDays){
            OutputFileConfig config = OutputFileConfig
                    .builder()
                    .withPartPrefix("Q1-W-"+days)
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
                .filter(new RichFilterFunction<Tuple3<Long, Integer, Double>>() {

                    private transient Meter recordsInMeter;
                    private transient Meter recordsOutMeter;

                    @Override
                    public void open(Configuration parameters) {

                        recordsInMeter = getRuntimeContext()
                                .getMetricGroup()
                                .meter("Q1_filter_in", new MeterView(60)); // 60 seconds rate
                        recordsOutMeter = getRuntimeContext()
                                .getMetricGroup()
                                .meter("Q1_filter_out", new MeterView(60)); // 60 seconds rate
                    }

                    @Override
                    public boolean filter(Tuple3<Long, Integer, Double> tuple3) {
                        recordsInMeter.markEvent();

                        boolean result = tuple3.f1 >= 1000 && tuple3.f1 <= 1020;

                        if (result) {
                            recordsOutMeter.markEvent();
                        }

                        return result;
                    }
                });

        for (int days: ProjectUtils.windowsDays) {
            AtomicBoolean needHeader = headerMap.get(days);
            DataStream<Tuple5<Long, Integer, Integer, Double, Double>> results = filteredVault
                    .keyBy(t -> t.f1)
                    .window(
                            TumblingEventTimeWindows.of(Duration.ofDays(days)))
                    .process(
                            new TemperatureStatsMetrics())
                    .returns(Types.TUPLE(Types.LONG, Types.INT, Types.INT, Types.DOUBLE, Types.DOUBLE));

            //to put header and data to the output file removing the brackets
            results.map(
                    t -> {
                        String ret = t.toString().substring(1, t.toString().length() - 1);
                        if(needHeader.get()){
                            needHeader.set(false);
                            return HEADERS + "\n" +
                                    ret;
                        }
                        return ret;
                    }
            ).name("Q1-W-"+days).sinkTo(sinkMap.get(days));
            //results.print();
        }
    }
}