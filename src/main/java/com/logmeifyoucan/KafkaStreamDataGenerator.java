package com.logmeifyoucan;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/****************************************************************************
 * This Generator generates a series of data files in the raw_data folder
 * It is an audit trail data source.
 * This can be used for streaming consumption of data by Flink
 ****************************************************************************/

public class KafkaStreamDataGenerator implements Runnable {


    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_BLUE = "\u001B[34m";

    public static void main(String[] args) {
        KafkaStreamDataGenerator gen = new KafkaStreamDataGenerator();
        gen.run();
    }

    public void run() {

        //Setup Kafka Client
        Properties kafkaProps = setupKafkaClientProperties();

        Producer<String,String> myProducer = new KafkaProducer<>(kafkaProps);

        String kafkaTopic = "flink.kafka.streaming.source";
        String logFile = "logs/app.log";

        BufferedReader reader;
        Long previousUnixTime = null;
        int speedFactor = 1000;

        try {
            reader = new BufferedReader(new FileReader(logFile));
            String line = reader.readLine();

            while (line != null) {

                String eventTime = line.split(" ")[0];

                DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                Date result =  df.parse(eventTime);
                long unixEventTime = result.getTime();

                if (previousUnixTime == null) {
                    previousUnixTime = unixEventTime;
                }

                Thread.sleep((unixEventTime - previousUnixTime) / speedFactor);

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(kafkaTopic, String.valueOf(unixEventTime), line);

                myProducer.send(record).get();

                System.out.println(ANSI_PURPLE + "Kafka Log Producer: " + line  + ANSI_RESET);

                // read next line
                line = reader.readLine();
            }
            reader.close();

        } catch (IOException | ParseException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        System.out.println(ANSI_PURPLE + "FINISHED" + ANSI_RESET);

//
//            //Define list of users
//            List<String> appUser = new ArrayList<String>();
//            appUser.add("Tom");
//            appUser.add("Harry");
//            appUser.add("Bob");
//
//            //Define list of application operations
//            List<String> appOperation = new ArrayList<String>();
//            appOperation.add("Create");
//            appOperation.add("Modify");
//            appOperation.add("Query");
//            appOperation.add("Delete");
//
//            //Define list of application entities
//            List<String> appEntity = new ArrayList<String>();
//            appEntity.add("Customer");
//            appEntity.add("SalesRep");
//
//            //Define a random number generator
//            Random random = new Random();
//
//            //Generate 100 sample audit records, one per each file
//            for(int i=0; i < 100; i++) {
//
//                //Capture current timestamp
//                String currentTime = String.valueOf(System.currentTimeMillis());
//
//                //Generate a random user
//                String user = appUser.get(random.nextInt(appUser.size()));
//                //Generate a random operation
//                String operation = appOperation.get(random.nextInt(appOperation.size()));
//                //Generate a random entity
//                String entity= appEntity.get(random.nextInt(appEntity.size()));
//                //Generate a random duration for the operation
//                String duration = String.valueOf(random.nextInt(10) + 1 );
//                //Generate a random value for number of changes
//                String changeCount = String.valueOf(random.nextInt(4) + 1);
//
//                //Create a CSV Text array
//                String[] csvText = { String.valueOf(i), user, entity,
//                        operation, currentTime, duration, changeCount} ;
//
//
//                String recKey = String.valueOf(currentTime);
//                ProducerRecord<String, String> record =
//                        new ProducerRecord<String,String>(
//                                "flink.kafka.streaming.source",
//                                recKey,
//                                String.join(",", csvText)  );
//
//                RecordMetadata rmd = myProducer.send(record).get();
//
//                System.out.println(ANSI_PURPLE + "Kafka Stream Generator : Sending Event : "
//                        + String.join(",", csvText)  + ANSI_RESET);
//
//                //Sleep for a random time ( 1 - 3 secs) before the next record.
//                Thread.sleep(random.nextInt(2000) + 1);
//            }
//
//
//
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

    }

    private Properties setupKafkaClientProperties() {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers","localhost:9092");

        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return kafkaProps;
    }


}
