package class3;

import class1.ClickSource;
import class1.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class KeyedProcessFunctionExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = environment.addSource(new ClickSource());
        stream.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Event, Object>() {
                    @Override
                    public void processElement(Event value, KeyedProcessFunction<String, Event, Object>.Context ctx, Collector<Object> out) throws Exception {
                        long current = ctx.timerService().currentProcessingTime();
                        out.collect(ctx.getCurrentKey() + " Processing Time: " + new Timestamp(current));
                        ctx.timerService().registerProcessingTimeTimer(current + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + " Triggered: " + new Timestamp(timestamp));
                    }

                }).print();
        environment.execute();
    }
}
