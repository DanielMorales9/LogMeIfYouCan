package com.logmeifyoucan.helper;

import com.logmeifyoucan.model.LogPoint;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;

public class WatermarkStrategyHelper {
    public static WatermarkStrategy<LogPoint> createWatermarkStrategy() {
        return WatermarkStrategy.<LogPoint>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.getEvenTime())
                .withIdleness(Duration.ofMinutes(1));
    }
}
