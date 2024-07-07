package dissanuddinahmed.utils;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public class KafkaCons {

    private static final String SINK_TO_FS = "Sink-to-filesystem";

    private final Consumer<String, String> consumer;
    public static final String FIRST_QUERY = "Q1-";
    public static final String SECOND_QUERY = "Q2-";
    private static final String WINDOW = "W-";
    private static final String outputDir = "Results/";
    public static final String[] TOPICS = {
            FIRST_QUERY+WINDOW+1,
            FIRST_QUERY+WINDOW+3,
            FIRST_QUERY+WINDOW+23,
            SECOND_QUERY+WINDOW+1,
            SECOND_QUERY+WINDOW+3,
            SECOND_QUERY+WINDOW+23
    };
    private static final Logger LOGGER = Logger.getLogger(KafkaProd.class.getSimpleName());

    public KafkaCons() {
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaCommon.BROKER_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, SINK_TO_FS);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        this.consumer = new KafkaConsumer<>(properties);
        this.consumer.subscribe(Arrays.asList(TOPICS));
    }



    public void consume() throws IOException {
        Map<String, BufferedWriter> writerMap = new HashMap<>();
        while (true){
            ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record: records){

                if (writerMap.get(record.topic()) == null){
                    writerMap.put(record.topic(),
                            new BufferedWriter(new FileWriter(outputDir+record.topic()+".csv")));
                }
                System.out.println(record.topic()+"\n"+record.value());
                writerMap.get(record.topic()).write(record.value());
            }
        }
    }
}
