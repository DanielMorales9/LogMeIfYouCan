package com.logmeifyoucan;

import com.logmeifyoucan.common.Constants;
import com.logmeifyoucan.common.Parser;
import com.logmeifyoucan.generator.KafkaStreamDataGenerator;
import com.logmeifyoucan.helper.FlinkKafkaConsumerHelper;
import com.logmeifyoucan.helper.StreamExecutionEnvHelper;
import com.logmeifyoucan.helper.WatermarkStrategyHelper;
import com.logmeifyoucan.model.LogPoint;
import com.logmeifyoucan.model.PageRequest;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

public class LoggingProcessing {

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
                        prettyPrint(s.toString());
                        return s;
                    })
                    .assignTimestampsAndWatermarks(generator);

            // Compute the count of events, minimum timestamp and maximum timestamp
            DataStream<Tuple4<String, Integer, Long, Long>> countSummary = logPointStream
                    .map(logPoint -> new Tuple4<>
                            (String.valueOf(System.currentTimeMillis()), 1, logPoint.getLogTime(), logPoint.getLogTime()))
                    .returns(Types.TUPLE(Types.STRING, Types.INT, Types.LONG, Types.LONG))
                    .timeWindowAll(Time.minutes(5))
                    .reduce((x, y) -> new Tuple4<>(x.f0, x.f1 + y.f1, Math.min(x.f2, y.f2), Math.max(x.f3, y.f3)));

            // pretty print
            countSummary.map((MapFunction<Tuple4<String, Integer, Long, Long>, Object>) slidingSummary1 -> {

                SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");

                String minTime = format.format(new Date(slidingSummary1.f2));
                String maxTime = format.format(new Date(slidingSummary1.f3));

                System.out.println(Constants.ANSI_GREEN
                        + "Count Event Summary: "
                        + "now=" + (new Date()).toString()
                        + ", Start Time: " + minTime
                        + ", End Time: " + maxTime
                        + ", Count: " + slidingSummary1.f1
                        + Constants.ANSI_RESET);

                return null;
            });


            DataStream<PageRequest> pageRequestStream = logPointStream
                    .filter(logPoint -> logPoint.getMsg().matches("Method: .*Resource: .*Duration: .*"))
                    .map(Parser::parseLogPoint)
                    .filter(p -> p != null)
                    .map(p -> {
                        prettyPrint(p.toString());
                        return p;
                    });

            DataStream<Tuple4<String, Integer, Long, Long>> pageLatencySummary = pageRequestStream
                    .map(page -> new Tuple4<>(page.getPage(), 1, page.getDuration(), page.getDuration()))
                    .returns(Types.TUPLE(Types.STRING, Types.INT, Types.LONG, Types.LONG))
                    .keyBy(p -> p.f0)
                    .timeWindow(Time.minutes(5))
                    .reduce((x, y) -> new Tuple4<>(x.f0, x.f1 + y.f1, Math.min(x.f2, y.f2), Math.max(x.f3, y.f3)));

            pageLatencySummary.map((MapFunction<Tuple4<String, Integer, Long, Long>, Object>) summary -> {

                System.out.println(Constants.ANSI_BLUE
                        + "Page Latency Summary: "
                        + " Min Time: " + summary.f2
                        + ", Max Time: " + summary.f3
                        + ", Count: " + summary.f1
                        + ", Page: " + summary.f0
                        + Constants.ANSI_RESET);

                return null;
            });

            //Start the Kafka Stream generator on a separate thread
            Thread kafkaThread = new Thread(new KafkaStreamDataGenerator());
            kafkaThread.start();

            // execute the streaming pipeline
            streamEnv.execute("Flink Log Analysis");
        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }

    private static void prettyPrint(String message) {
        System.out.println("Flink: " + message);
    }


}
