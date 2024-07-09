package dissanuddinahmed;

import dissanuddinahmed.queries.FirstQuery;
import dissanuddinahmed.queries.SecondQuery;
import dissanuddinahmed.utils.KafkaUtils;
import dissanuddinahmed.utils.ProjectUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.logging.Logger;

public class FlinkStream {
    private static final Logger LOGGER = Logger.getLogger("Logger");

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        long query = 0;
        boolean checkpointing = false;

        for (int i = 0; i < args.length; i++) {
            try {

                if (args[i].equals("--query")) {
                    query = Long.parseLong(args[i + 1]);
                    if (query < 0 || query > 2){
                        throw new NumberFormatException(query + " Not allowed " + "pass 1 or 2");
                    }
                }

                if (args[i].equals("--parallelism")){
                    ProjectUtils.PARALLELISM.set(Integer.parseInt(args[i+1]));
                }

                if (args[i].equals("--checkpointing")){
                    checkpointing = true;
                }

            } catch (ArrayIndexOutOfBoundsException e) {
                LOGGER.warning("Passed to many arguments");
            } catch (NumberFormatException | NullPointerException e) {
                LOGGER.warning("Passed bad argument for " + args[i]);
                System.exit(-1);
            }
        }

        env.getConfig().setLatencyTrackingInterval(10);



        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(KafkaUtils.KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(KafkaUtils.FLINK_TOPIC)
                .setGroupId(KafkaUtils.CONSUMER_GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> sourceStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka-producer"
        ).setParallelism(ProjectUtils.PARALLELISM.get());

        if (checkpointing){
            // start a checkpoint every 2 minute
            env.enableCheckpointing(120000);

            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            // 500 ms of progress happen between checkpoints
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
            // checkpoints have to complete within one minute, or are discarded
            env.getCheckpointConfig().setCheckpointTimeout(60000);
            // only two consecutive checkpoint failures are tolerated
            env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
            //checkpoint to be in progress at the same time
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(
                ProjectUtils.PARALLELISM.get()
            );
            // enables the unaligned checkpoints
            if (ProjectUtils.PARALLELISM.get() == 1) {
                env.getCheckpointConfig().enableUnalignedCheckpoints();
            }
            //storage where checkpoint snapshots will be written
            Configuration config = new Configuration();
            config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
            config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///tmp/dspflink/checkpoint");
            env.configure(config);

        }


        if (query == 0) {
            FirstQuery.launch(sourceStream);
            SecondQuery.launch(sourceStream);
        }else if(query == 1) {
            FirstQuery.launch(sourceStream);
        }else {
            SecondQuery.launch(sourceStream);
        }

        env.execute(FlinkStream.class.getSimpleName());
    }

}