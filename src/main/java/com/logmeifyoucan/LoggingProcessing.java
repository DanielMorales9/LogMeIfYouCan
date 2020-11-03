package com.logmeifyoucan;

import com.logmeifyoucan.common.Constants;
import com.logmeifyoucan.generator.KafkaStreamDataGenerator;
import com.logmeifyoucan.model.LogPoint;
import com.logmeifyoucan.model.PageRequest;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LoggingProcessing {

    public static final String GROUP_ID = "flink.learn.realtime";

    public static void main(String[] args) {

        try {

            // Set up the streaming execution environment
            final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
            streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            WatermarkStrategy<LogPoint> generator =
                    WatermarkStrategy.<LogPoint>forMonotonousTimestamps()
                            .withTimestampAssigner((event, timestamp) -> event.getEvenTime())
                            .withIdleness(Duration.ofMinutes(1));

            //Set connection properties to Kafka Cluster
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
            properties.setProperty("group.id", GROUP_ID);

            //Setup a Kafka Consumer on Flink
            FlinkKafkaConsumer<String> kafkaConsumer =
                    new FlinkKafkaConsumer<>(Constants.KAFKA_SOURCE_TOPIC, new SimpleStringSchema(), properties);

            //Setup to receive only new messages
            kafkaConsumer.setStartFromLatest();

            //Create the data stream
            DataStream<String> logStream = streamEnv.addSource(kafkaConsumer);

            //Convert each record to an Object
            DataStream<LogPoint> logPointStream = logStream
                    .map(LoggingProcessing::parseLogMessage)
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
                    .map(LoggingProcessing::parseLogPoint)
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

    private static PageRequest parseLogPoint(LogPoint logPoint) {
        String actualLogMessage = logPoint.getMsg();

        Pattern p = Pattern.compile("Method: (.*) Resource: (.*) Duration: (.*)");
        Matcher m = p.matcher(actualLogMessage);

        if (m.find() && m.groupCount() >= 3) {
            String method = m.group(1);
            String page = m.group(2);
            Long duration = Long.valueOf(m.group(3));
            return new PageRequest(logPoint.getEvenTime(), method, page, duration);
        }
        else {
            prettyPrint("ERROR: " + actualLogMessage);
            return null;
        }
    }

    private static LogPoint parseLogMessage(String logMessage) throws ParseException {
        Pattern p = Pattern.compile(Constants.LOG_REGEX);
        Matcher m = p.matcher(logMessage);

        if (m.find() && m.groupCount() >= 7) {
            String logDate = m.group(1);
            String eventDate = m.group(2);
            String level = m.group(3);
            String thread = m.group(4);
            String pid = m.group(5);
            String className = m.group(6);
            String msg = m.group(7);

            DateFormat df;
            df = new SimpleDateFormat(Constants.SIMPLE_DATE_FORMAT);
            long logTime = df.parse(logDate).getTime();
            df = new SimpleDateFormat(Constants.SIMPLE_DATE_FORMAT2);
            long eventTime = df.parse(eventDate).getTime();

            return new LogPoint(logTime, eventTime, level, thread, pid, className, msg);
        }
        else {
            prettyPrint("ERROR: " + logMessage);
            return null;
        }


    }

    private static void prettyPrint(String message) {
        System.out.println("Flink: " + message);
    }


}
