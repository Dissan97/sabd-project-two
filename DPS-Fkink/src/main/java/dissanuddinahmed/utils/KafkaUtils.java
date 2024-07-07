package dissanuddinahmed.utils;

public class KafkaUtils {
    //"kafka-1:19092,kafka-2:19093,kafka-3:19094"
    public static final String KAFKA_BOOTSTRAP_SERVERS = System.getenv("KAFKA_BOOTSTRAP_SERVERS")
            .replaceAll("\n", "");
    //"dsp-flink"
    public static final String FLINK_TOPIC = System.getenv("FLINK_TOPIC")
            .replaceAll("\n", "");
    //"dsp-consumer"
    public static final String CONSUMER_GROUP_ID = System.getenv("CONSUMER_GROUP_ID")
            .replaceAll("\n", "");



}
