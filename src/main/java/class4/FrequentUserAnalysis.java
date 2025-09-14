package class4;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class FrequentUserAnalysis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<UserBehavior> stream = environment.readTextFile("C:\\Users\\admin\\IdeaProjects\\flink-2025-sep\\src\\main\\resources\\UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new UserBehavior(arr[0], arr[1], arr[2], arr[3], Long.parseLong(arr[4]) * 1000L);
                    }
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        stream.keyBy(data -> data.userId)
                .process(new FrequentUserResult())
                .print();
        environment.execute();
    }

    public static class FrequentUserResult extends KeyedProcessFunction<String, UserBehavior, String> {

        private transient ValueState<Long> visitCount;
        private transient ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            visitCount = getRuntimeContext().getState(new ValueStateDescriptor<>("visitCount", Long.class, 0L));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<>("timerstate", Long.class));
        }

        @Override
        public void processElement(UserBehavior value, KeyedProcessFunction<String, UserBehavior, String>.Context ctx, Collector<String> out) throws Exception {
            long count = visitCount.value() + 1;
            visitCount.update(count);
            if (timerState.value() == null) {
                long midNight = (ctx.timestamp() / (24 * 60 * 60 * 1000) + 1) * (24 * 60 * 60 * 1000);
                ctx.timerService().registerEventTimeTimer(midNight);
                timerState.update(midNight);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            long count = visitCount.value();
            if (count > 2) {
                out.collect("Frequent User: " + ctx.getCurrentKey() + " visit Count: " + count);
            }
            visitCount.clear();
            timerState.clear();
        }
    }
}
