package dissanuddinahmed.utils;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;

public class KafkaProd extends Thread{


    private final String name;
    private String filename;
    private final Producer<String, String> producer;
    private static final int MINI_BATCH_SIZE = 500;
    public static final String TOPIC = "dsp-flink";
    private static final Logger LOGGER = Logger.getLogger(KafkaProd.class.getSimpleName());
    // FINAL_FAKE_TUPLE_NEEDED_TO_TRIGGER_ALL_WINDOW"
    private static final String FINAL_TUPLE1 ="1682380800000,Z304JGGF,ST4000DM000,1,1010,1.5452484E8,,0.0,23.0,0.0,"+
            "5.33028348E8,,66090.0,0.0,23.0,,,0.0,0.0,0.0,0.0,23.0,0.0,0.0,7664.0,23.0,,,0.0,0.0,0.0,,,,,,65983.0,"+
            "7.0908545888E10,4.94415938E11";

    private static final String FINAL_TUPLE2 ="1683676800000,Z304JGGF,ST4000DM000,1,1011,1.5452484E8,,0.0,23.0,0.0,"+
        "5.33028348E8,,66090.0,0.0,23.0,,,0.0,0.0,0.0,0.0,23.0,0.0,0.0,7664.0,23.0,,,0.0,0.0,0.0,,,,,,65983.0,"+
        "7.0908545888E10,4.94415938E11";

    public KafkaProd(String tName, String path){
        this.name = tName;
        this.filename = path;
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaCommon.BROKER_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(properties);
    }



    public void produceData(){

        try (BufferedReader reader = new BufferedReader(new FileReader(filename))){
            /*String header = */reader.readLine();


            String line = null;
            StringBuilder batch;
            long start = System.nanoTime();
            long counter = 0L;
            long kafkaInterections = 0L;
            do {
                batch = new StringBuilder();
                for (int i = 0; i < MINI_BATCH_SIZE; i++) {

                    line = reader.readLine();
                    if (line == null) {
                        break;
                    }
                    batch.append(line).append('\n');

                    counter++;

                }

                this.produce(String.valueOf(batch.hashCode()), batch.toString());
                kafkaInterections ++;
            } while (line != null);

            long end = System.nanoTime();
            LOGGER.info(name + "sent: " + counter + " tuples\ntook: " + (end - start) /(Math.pow(10, 9)) + "s\n" +
                    "kafka interactions: " + kafkaInterections);
            Thread.sleep(6000);

            for (int i = 0; i < 5; i++) {

                this.produce(String.valueOf(System.currentTimeMillis() + i)
                    , FINAL_TUPLE1);
                this.produce(String.valueOf(System.currentTimeMillis()),
                    FINAL_TUPLE2);

            }

        }catch (ArrayIndexOutOfBoundsException e){
            LOGGER.warning(e.getMessage());
            LOGGER.info("Insert data-source-filename");
        }catch (IOException | InterruptedException e ){
            LOGGER.warning(e.getMessage());
            System.exit(-1);
        }
    }

    @Override
    public void run() {
        LOGGER.info(this.name + "Operating starting service");
        this.filename = this.filename +'-'+ this.name  + ".csv";
        this.produceData();

    }

    /**
     * Days contains mean: 130000 entries so sending 87 batches of 3 cluster per day
     * will take 20s:1day total 480s:23days SPEED FACTOR OF aprox: 4800
       @param value batch of 500 tuples
     */
    private void produce(String key, String value) throws RuntimeException {
            final ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);
            producer.send(record);
        try {
            Thread.sleep(240);
        } catch (InterruptedException e) {
            LOGGER.warning("some issue in Thread.sleep" + e.getMessage());
        }
    }





}
