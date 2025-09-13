package class3;

import class1.ClickSource;
import class1.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

public class WindowUniqueUrlViewCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = environment.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );
        stream.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new UniqueUrlViewAggregator(), new UniqueUrlViewWindowFunction())
                .print();
        environment.execute();
    }
    public static class UniqueUrlViewAggregator implements AggregateFunction<Event, HashSet<String>, Long> {
        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<>();
        }
        @Override
        public HashSet<String> add(Event value, HashSet<String> accumulator) {
            accumulator.add(value.url);
            return accumulator;
        }
        @Override
        public Long getResult(HashSet<String> accumulator) {
            return (long) accumulator.size();
        }
        @Override
        public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
            return null;
        }
    }
    public static class UniqueUrlViewWindowFunction extends ProcessWindowFunction<Long, String, Boolean, TimeWindow> {

        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<Long, String, Boolean, TimeWindow>.Context context,
                            Iterable<Long> elements, Collector<String> out) throws Exception {
            long uniqueUrlView = elements.iterator().next();
            long start = context.window().getStart();
            long end = context.window().getEnd();
            out.collect("--------------------------------------------------------------------------------------\n");
            out.collect("Start Time: " + new Timestamp(start) + "      End Time: " + new Timestamp(end) + " Unique Url View Count: " + uniqueUrlView);
            out.collect("--------------------------------------------------------------------------------------\n");
        }
    }

}
