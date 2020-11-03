package com.logmeifyoucan;

import com.logmeifyoucan.common.Parser;
import com.logmeifyoucan.generator.KafkaStreamDataGenerator;
import com.logmeifyoucan.helper.FlinkKafkaConsumerHelper;
import com.logmeifyoucan.helper.StreamExecutionEnvHelper;
import com.logmeifyoucan.helper.WatermarkStrategyHelper;
import com.logmeifyoucan.model.LogPoint;
import com.logmeifyoucan.model.PageRequest;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBConfig;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBPoint;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class LoggingInfluxDB {

    public static final String GROUP_ID = "flink.learn.realtime";
    private static final String HOST = "http://localhost:8086";
    private static final String USERNAME = "flink";
    private static final String PASSWORD = "flinkrocks";
    private static final String DATABASE = "page_request";

    public static void main(String[] args) {

        try {

            // Set up the streaming execution environment
            final StreamExecutionEnvironment streamEnv = StreamExecutionEnvHelper.createStreamExecutionEnvironment();

            //Setup a Kafka Consumer on Flink
            FlinkKafkaConsumer<String> kafkaConsumer = FlinkKafkaConsumerHelper.createConsumer();

            //Setup to receive only new messages
            kafkaConsumer.setStartFromLatest();

            //Create the data stream
            DataStream<String> logStream = streamEnv.addSource(kafkaConsumer);


            WatermarkStrategy<LogPoint> generator = WatermarkStrategyHelper.createWatermarkStrategy();

            //Convert each record to an Object
            DataStream<LogPoint> logPointStream = logStream
                    .map(Parser::parseLogMessage)
                    .filter(Objects::nonNull)
                    .map(s -> {
                        LoggingInfluxDB.prettyPrint(s.toString());
                        return s;
                    })
                    .assignTimestampsAndWatermarks(generator);

            DataStream<PageRequest> pageRequestStream = logPointStream
                    .filter(logPoint -> logPoint.getMsg().matches("Method: .*Resource: .*Duration: .*"))
                    .map(Parser::parseLogPoint)
                    .filter(p -> p != null)
                    .map(p -> {
                        LoggingInfluxDB.prettyPrint(p.toString());
                        return p;
                    });

            DataStream<InfluxDBPoint> influxPoints = pageRequestStream
                    .map(pageRequest -> {

                        HashMap<String, String> tags = new HashMap<>();
                        tags.put("method", pageRequest.getMethod());
                        tags.put("page", pageRequest.getPage());


                        HashMap<String, Object> fields = new HashMap<>();
                        fields.put("duration", pageRequest.getDuration());

                        return new InfluxDBPoint("page", pageRequest.getEventTime(), tags, fields);
                    });


            InfluxDBConfig influxDBConfig = InfluxDBConfig.builder(HOST, USERNAME, PASSWORD, DATABASE)
                    .batchActions(1000)
                    .flushDuration(1, TimeUnit.MINUTES)
                    .build();

            influxPoints.addSink(new InfluxDBSink(influxDBConfig));


            //Start the Kafka Stream generator on a separate thread
            Thread kafkaThread = new Thread(new KafkaStreamDataGenerator());
            kafkaThread.start();

            // execute the streaming pipeline
            streamEnv.execute("Flink Bulk loading into InfluxDB");
        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }

    private static void prettyPrint(String message) {
        System.out.println("Flink: " + message);
    }


}
