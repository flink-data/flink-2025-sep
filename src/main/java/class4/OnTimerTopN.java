package class4;

import class1.ClickSource;
import class1.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
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

public class OnTimerTopN {

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
        SingleOutputStreamOperator<UrlViewCount> urlCountStream = stream
                .keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(5)))
                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult()
                );
        urlCountStream
                .keyBy(data -> data.windowEnd)
                        .process(new TopNProcess(5))
                                .print();
        environment.execute();
    }

    public static class UrlViewCount {
        public String url;
        public long count;
        public long windowStart;
        public long windowEnd;
        public UrlViewCount(String url, long count, long windowStart, long windowEnd) {
            this.url = url;
            this.count = count;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }
        public UrlViewCount() {
        }
        @Override
        public String toString() {
            return "UrlViewCount{" +
                    "url='" + url + '\'' +
                    ", count=" + count +
                    ", windowStart=" + windowStart +
                    ", windowEnd=" + windowEnd +
                    '}';
        }
    }
    public static class UrlViewCountAgg implements AggregateFunction<Event, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
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

    public static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {

        @Override
        public void process(String url, ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>.Context context,
                            Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            long count = elements.iterator().next();
            out.collect(new UrlViewCount(url, count, context.window().getStart(), context.window().getEnd()));
        }
    }

    public static class TopNProcess extends KeyedProcessFunction<Long, UrlViewCount, String> {

        private final int topSize;
        private transient ListState<UrlViewCount> urlListState;

        public TopNProcess(int topSize) {
            this.topSize = topSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<UrlViewCount> descriptor = new ListStateDescriptor<>("url-list-state", UrlViewCount.class);
            urlListState = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void processElement(UrlViewCount value, KeyedProcessFunction<Long, UrlViewCount, String>.Context ctx, Collector<String> out) throws Exception {
            urlListState.add(value);
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<UrlViewCount> all = new ArrayList<>();
            for (UrlViewCount  u : urlListState.get()) {
                all.add(u);
            }
            urlListState.clear();
            all.sort(Comparator.comparingLong((UrlViewCount u) -> u.count).reversed());
            StringBuilder sb = new StringBuilder();
            sb.append("=================================================================\n");
            sb.append("Window End: ").append(new Timestamp(ctx.getCurrentKey())).append("\n");
            for (int i = 0; i < Math.min(topSize, all.size()); i++) {
                UrlViewCount current = all.get(i);
                sb.append("No. ").append(i + 1)
                        .append(" Url = ").append(current.url)
                        .append(" Count = ").append(current.count)
                        .append("\n");
            }
            sb.append("=================================================================\n");
            out.collect(sb.toString());
        }
    }
}
