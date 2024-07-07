package dissanuddinahmed.utils;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple;

import java.text.DecimalFormat;
import java.time.Duration;
import java.util.List;

public class ProjectUtils {

    private ProjectUtils(){}

    public static final List<Integer> windowsDays = List.of(
            1, 3, 23
    );

    public static <T extends Tuple> WatermarkStrategy<T> getWatermarkStrategy() {
        return WatermarkStrategy.<T>forBoundedOutOfOrderness(Duration.ofDays(2))
                .withTimestampAssigner(
                        (SerializableTimestampAssigner<T>) (element, recordTimestamp) -> element.getField(0));
    }


    public static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.###");

}
