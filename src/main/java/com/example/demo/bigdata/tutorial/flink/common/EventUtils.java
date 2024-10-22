package com.example.demo.bigdata.tutorial.flink.common;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Description:
 * @Author: Chenyang on 2024/10/21 11:08
 * @Version: 1.0
 */
public class EventUtils {

    public static List<SourceEvent1> getEvent1List() {
        List<SourceEvent1> list = new ArrayList<>();
        list.add(new SourceEvent1(1, "Jean", 1000L));
        list.add(new SourceEvent1(2, "Sean", 3000L));
        list.add(new SourceEvent1(3, "Tom", 7000L));
        list.add(new SourceEvent1(4, "Jeremy", 2000L));
        list.add(new SourceEvent1(5, "Sean", 2000L));
        list.add(new SourceEvent1(6, "Joe", 1000L));
        list.add(new SourceEvent1(7, "Jean", 5000L));
        list.add(new SourceEvent1(8, "Mary", 6000L));
        list.add(new SourceEvent1(9, "Jeremy", 1000L));
        list.add(new SourceEvent1(10, "Jean", 1000L));
        list.add(new SourceEvent1(11, "Tom", 5000L));
        list.add(new SourceEvent1(12, "Mary", 9000L));
        list.add(new SourceEvent1(13, "Jeremy", 6000L));
        list.add(new SourceEvent1(14, "Joe", 3000L));
        return list;
    }

    public static List<SourceEvent2> getEvent2List() {
        List<SourceEvent2> list = new ArrayList<>();
        list.add(new SourceEvent2("Mary", "/home", 1500L));
        list.add(new SourceEvent2("Alice", "/home", 1000L));
        list.add(new SourceEvent2("Bob", "/home", 2000L));
        list.add(new SourceEvent2("Mary", "/prod", 3000L));
        list.add(new SourceEvent2("Bob", "/prod", 3300L));
        list.add(new SourceEvent2("Alice", "/home", 1000L));
        list.add(new SourceEvent2("Alice", "/home", 1000L));
        list.add(new SourceEvent2("Bob", "/cart", 3500L));
        list.add(new SourceEvent2("Bob", "/prod?id=2", 3200L));
        list.add(new SourceEvent2("Mary", "/cart", 3800L));
        list.add(new SourceEvent2("Mary", "/prod?id=3", 5200L));
        list.add(new SourceEvent2("Mary", "/prod?id=4", 4200L));
        return list;
    }

    public static List<FlinkEvent> getFlinkEventList() {
        List<FlinkEvent> list = new ArrayList<>();
        list.add(new FlinkEvent("1", "Jeremy", 27));
        list.add(new FlinkEvent("2", "Sean", 18));
        list.add(new FlinkEvent("3", "Melissa", 26));
        list.add(new FlinkEvent("4", "Tom", 30));
        return list;
    }

    public static MySourceFunction2 getSourceFunction2() {
        return new MySourceFunction2();
    }

    public static class MySourceFunction2 implements ParallelSourceFunction<SourceEvent2> {
        private boolean running = true;

        @Override
        public void run(SourceContext<SourceEvent2> ctx) throws Exception {
            Random random = new Random();

            String[] names = new String[]{
                    "Jeremy",
                    "Sean",
                    "Mary",
                    "Jason",
                    "Jean",
                    "Tommy"
            };
            String[] urls = new String[]{
                    "/prod?id=1",
                    "/prod?id=2",
                    "/home",
                    "/index",
                    "/cart"
            };
            
            while (running) {
                ctx.collect(new SourceEvent2(names[random.nextInt(names.length)],
                        urls[random.nextInt(urls.length)],
                        Calendar.getInstance().getTimeInMillis()));
//                // 提交时间戳
//                ctx.collectWithTimestamp(sourceEvent2,sourceEvent2.getTimestamp());
//                // 发出watermark
//                ctx.emitWatermark(new Watermark(sourceEvent2.getTimestamp()));

                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    // 每秒生成一个对象
    public static MySourceFunction getSourceFunction() {
        return new MySourceFunction();
    }

    public static class MySourceFunction extends RichParallelSourceFunction<FlinkEvent> {
        private Random random = new Random();
        private boolean running = true;

        @Override
        public void run(SourceContext<FlinkEvent> ctx) throws Exception {
            String[] names = new String[]{
                    "Jeremy",
                    "Sean",
                    "Mary",
                    "Jason",
                    "Edward",
                    "Tommy"
            };
            AtomicInteger count = new AtomicInteger(0);
            while (running) {
                FlinkEvent flinkEvent = new FlinkEvent();
                flinkEvent.setId(count.incrementAndGet() + "");
                flinkEvent.setName(names[random.nextInt(names.length)]);
                flinkEvent.setAge(random.nextInt(100));
                ctx.collect(flinkEvent);

                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    // 自定义水位线策略
    public static class MyWatermarkStrategy implements WatermarkStrategy<SourceEvent2> {

        private static WatermarkGenerator<SourceEvent2> watermarkGenerator;

        public MyWatermarkStrategy(WatermarkGenerator<SourceEvent2> watermarkGenerator) {
            super();
            this.watermarkGenerator = watermarkGenerator;
        }

        // 水位线生成逻辑
        @Override
        public WatermarkGenerator<SourceEvent2> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return watermarkGenerator;
        }

        // 时间戳分配逻辑
        @Override
        public TimestampAssigner<SourceEvent2> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return (element, recordTimestamp) -> element.getTimestamp();
        }
    }

    // 自定义周期性水位线生成器
    public static class MyPeriodicWatermarkGenerator implements WatermarkGenerator<SourceEvent2> {

        // 延迟时间
        private long delayTime = 5000L;
        // 最大时间戳
        private long maxTs = Long.MIN_VALUE + delayTime + 1L;

        // 断点式
        @Override
        public void onEvent(SourceEvent2 event, long eventTimestamp, WatermarkOutput output) {
            // 每来一条数据就调用一次
            maxTs = Math.max(maxTs, event.getTimestamp());
        }

        // 周期性
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 最大时间-延迟时间-1ms
            output.emitWatermark(new Watermark(maxTs - delayTime - 1L));
        }
    }

    // 自定义断点式水位线生成器
    public static class MyPunctuatedWatermarkGenerator implements WatermarkGenerator<SourceEvent2> {

        @Override
        public void onEvent(SourceEvent2 event, long eventTimestamp, WatermarkOutput output) {
            // 符合条件就发出水位线
            if (event.getName().startsWith("J")) {
                output.emitWatermark(new Watermark(event.getTimestamp() - 1L));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 无需执行
        }
    }

}
