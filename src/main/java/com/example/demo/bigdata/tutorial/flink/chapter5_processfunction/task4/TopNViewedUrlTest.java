package com.example.demo.bigdata.tutorial.flink.chapter5_processfunction.task4;

import com.example.demo.bigdata.tutorial.flink.common.EventUtils;
import com.example.demo.bigdata.tutorial.flink.common.SourceEvent2;
import com.example.demo.bigdata.tutorial.flink.common.UrlCountEntity;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @Description: 每五秒统计十秒内topN
 * @Author: Chenyang on 2024/10/23 17:35
 * @Version: 1.0
 */
public class TopNViewedUrlTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<SourceEvent2> dataStream = env.addSource(EventUtils.getSourceFunction2())
                .assignTimestampsAndWatermarks(EventUtils.getMyPeriodicWatermarkStrategy(0L));

        dataStream.keyBy(SourceEvent2::getUrl)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new myAggregateFunction(), new MyProcessWindowFunction())
                .keyBy(UrlCountEntity::getWindowEnd)
                .process(new TopN(2))
                .print();

        env.execute();

    }

    // 对于不同的key, 定时器和状态都是独立的
    public static class TopN extends KeyedProcessFunction<Long, UrlCountEntity, String> {

        // top个数
        private int n;
        // 状态列表
        private ListState<UrlCountEntity> listState;

        public TopN(int n) {
            this.n = n;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 获取列表状态句柄
            listState = getRuntimeContext()
                    .getListState(new ListStateDescriptor<>("state-list", Types.POJO(UrlCountEntity.class)));
        }

        @Override
        public void processElement(UrlCountEntity value, Context ctx, Collector<String> out) throws Exception {
            // 将数据添加到列表状态中
            listState.add(value);
            // 注册ET定时器
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            List<UrlCountEntity> list = new ArrayList<>();
            // 取出保存的状态
            for (UrlCountEntity urlCountEntity : listState.get()) {
                list.add(urlCountEntity);
            }

            // 清空状态
            listState.clear();

            // 降序
            Collections.sort(list);

            // 拼接输出
            StringBuilder sb = new StringBuilder();
            sb.append("============================\n");
            // 防止n大于窗口中生成的url种类
            int size = Math.min(n, list.size());
            for (int i = 0; i < size; ++i) {
                UrlCountEntity urlCountEntity = list.get(i);
                String info = "No." + (i + 1) + " "
                        + "url: " + urlCountEntity.getUrl() + " "
                        + "浏览量: " + urlCountEntity.getCount() + "\n";
                sb.append(info);
            }
            sb.append("============================\n");
            out.collect(sb.toString());
        }
    }

    public static class myAggregateFunction implements AggregateFunction<SourceEvent2, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(SourceEvent2 value, Long accumulator) {
            return ++accumulator;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    public static class MyProcessWindowFunction extends ProcessWindowFunction<Long, UrlCountEntity, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<UrlCountEntity> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            UrlCountEntity urlCountEntity = new UrlCountEntity(s, elements.iterator().next(), start, end);
            out.collect(urlCountEntity);
            System.out.println(urlCountEntity);
        }
    }
}
