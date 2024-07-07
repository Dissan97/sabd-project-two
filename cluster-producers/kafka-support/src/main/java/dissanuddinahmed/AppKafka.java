package dissanuddinahmed;

import dissanuddinahmed.utils.KafkaCons;
import dissanuddinahmed.utils.KafkaProd;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

public class AppKafka {


    private static final Logger LOGGER = Logger.getLogger(AppKafka.class.getSimpleName());



    public static void runProducer(String path, boolean cluster){
            if (cluster) {


                    List<Thread> clusters = List.of(
                            new KafkaProd("cluster0", path),
                            new KafkaProd("cluster1", path),
                            new KafkaProd("cluster2", path)
                    );

                    for (Thread  thread: clusters) {
                            thread.start();
                    }
                    for (Thread thread : clusters) {
                            try {
                                    thread.join();
                            } catch (InterruptedException e) {
                                    LOGGER.warning(e.getMessage());
                            }
                    }
                    return;
            }


            KafkaProd prod = new KafkaProd("SingleCluster", path);
            prod.produceData();
    }


    private static void runConsumer() {
            try {
                    KafkaCons cons = new KafkaCons();
                    cons.consume();
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
    }

    private final static String  ARGS_MSG = "Pass {arg0: consume} or"+
            "{arg0: produce, arg1: disk-failure-path, arg1: disk-failure-path, arg2: cluster}";

    public static void main(String[] args) {
            boolean isCluster = false;
            if (args.length == 0){
                    LOGGER.warning(ARGS_MSG);
                    System.exit(-1);
            }
            try {
                    if (args[0].equalsIgnoreCase("consume")){
                            runConsumer();
                    } else if (args[0].equalsIgnoreCase("produce")){
                            if (args.length > 2){
                                if (args[2].equalsIgnoreCase("cluster")){
                                            isCluster = true;
                                    }
                            }

                            runProducer(args[1], isCluster);

                    }
            }catch (ArrayIndexOutOfBoundsException e){
                    LOGGER.warning(e.getMessage() + ARGS_MSG);
            }
    }


}