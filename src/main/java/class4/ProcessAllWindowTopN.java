package class4;

import class1.ClickSource;
import class1.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

public class ProcessAllWindowTopN {

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
        stream.keyBy(data -> data.url)
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(5)))
                .aggregate(new UrlCountAggregator(), new UrlCountResult())
                .print();
        environment.execute();
    }

    public static class UrlCountAggregator implements AggregateFunction<Event, HashMap<String,Long>, ArrayList<Tuple2<String, Long>>> {
        @Override
        public HashMap<String, Long> createAccumulator() {
            return new HashMap<>();
        }
        @Override
        public HashMap<String, Long> add(Event value, HashMap<String, Long> accumulator) {
            if (accumulator.containsKey(value.url)) {
                Long count = accumulator.get(value.url);
                accumulator.put(value.url, count + 1);
            } else {
                accumulator.put(value.url, 1L);
            }
            return accumulator;
        }
        @Override
        public ArrayList<Tuple2<String, Long>> getResult(HashMap<String, Long> accumulator) {
            ArrayList<Tuple2<String, Long>> result = new ArrayList<>();
            for (String url : accumulator.keySet()) {
                result.add(Tuple2.of(url, accumulator.get(url)));
            }
            result.sort(new Comparator<Tuple2<String, Long>>() {
                @Override
                public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                    return o2.f1.intValue() - o1.f1.intValue();
                }
            });
            return result;
        }

        @Override
        public HashMap<String, Long> merge(HashMap<String, Long> a, HashMap<String, Long> b) {
            return null;
        }
    }

    public static class UrlCountResult extends ProcessAllWindowFunction<ArrayList<Tuple2<String, Long>>, String, TimeWindow> {

        @Override
        public void process(ProcessAllWindowFunction<ArrayList<Tuple2<String, Long>>, String, TimeWindow>.Context context,
                            Iterable<ArrayList<Tuple2<String, Long>>> elements, Collector<String> out) throws Exception {
            ArrayList<Tuple2<String, Long>> result = elements.iterator().next();
            StringBuilder sb = new StringBuilder();
            sb.append("==========================================================================\n");
            for (int i = 0; i < result.size() && i < 5; i++) {
                Tuple2<String, Long> current = result.get(i);
                String info = "\n" + "No. " + (i + 1) + " Url: " + current.f0 + " Total Views: " + current.f1 + " \n";
                sb.append(info);
            }
            sb.append("==========================================================================\n");
            out.collect(sb.toString());
        }
    }
}
