package class4;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

public class PVTopN {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<UserBehavior> stream = environment.readTextFile("C:\\Users\\admin\\IdeaProjects\\flink-2025-sep\\src\\main\\resources\\UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new UserBehavior(arr[0],arr[1],arr[2],arr[3], Long.parseLong(arr[4]) * 1000L);
                    }
                }).filter(data -> data.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        stream.keyBy(r -> r.itemId)
                .window(SlidingEventTimeWindows.of(Time.minutes(60), Time.minutes(5)))
                .aggregate(new CountAgg(), new ItemViewResult())
                        .keyBy(data -> data.windowEnd)
                                .process(new TopNItem(5))
                                        .print();
        environment.execute();
    }

    public static class ItemViewResult extends ProcessWindowFunction<Long, ItemViewCount, String, TimeWindow> {

        @Override
        public void process(String itemId, ProcessWindowFunction<Long, ItemViewCount, String, TimeWindow>.Context context,
                            Iterable<Long> elements, Collector<ItemViewCount> out) throws Exception {
            out.collect(new ItemViewCount(itemId, context.window().getEnd(), elements.iterator().next()));
        }
    }

    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    public static class TopNItem extends KeyedProcessFunction<Long, ItemViewCount, String> {

        private ListState<ItemViewCount> itemState;
        private Integer size;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            itemState = getRuntimeContext().getListState(new ListStateDescriptor<>("item", ItemViewCount.class));
        }

        public TopNItem(Integer size) {
            this.size = size;
        }

        @Override
        public void processElement(ItemViewCount value, KeyedProcessFunction<Long, ItemViewCount, String>.Context ctx,
                                   Collector<String> out) throws Exception {
            itemState.add(value);
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 100L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            ArrayList<ItemViewCount> allItems = new ArrayList<>();
            for (ItemViewCount item: itemState.get()) {
                allItems.add(item);
            }
            itemState.clear();
            allItems.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.count.intValue() - o1.count.intValue();
                }
            });
            StringBuilder sb = new StringBuilder();
            sb.append("=================================================================\n");
            sb.append("Window End: ").append(new Timestamp(timestamp)).append("\n");
            for (int i = 0; i < Math.min(allItems.size(), size); i++) {
                ItemViewCount current = allItems.get(i);
                sb.append(" No. ").append(i + 1).append( " ItemId = ").append(current.ItemId).append(" Count = ").append(current.count).append("\n");
            }
            sb.append("=================================================================\n");
            out.collect(sb.toString());
        }
    }
}
