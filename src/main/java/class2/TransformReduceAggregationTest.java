package class2;

import class1.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformReduceAggregationTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStreamSource<Event> stream = environment.fromElements(
                new Event("/cart", "Tom", 1000L ),
                new Event("/fav", "Tom", 2000L ),
                new Event("/home", "Ben", 3000L ),
                new Event("/order", "Nancy", 4000L ),
                new Event("/close", "Carl", 5000L ),
                new Event("/refund", "Tom", 600L ),
                new Event("/order", "Nancy", 4000L ),
                new Event("/home", "Nancy", 4000L ),
                new Event("/order", "Nancy", 4000L ),
                new Event("/refund", "Nancy", 4000L )
        );
        //number of clicks by each user
        SingleOutputStreamOperator<Tuple2<String, Long>> clicks = stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event value) throws Exception {
                return Tuple2.of(value.user, 1L);
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Long>> clickByEachUser = clicks.keyBy(tuple -> tuple.f0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                });
        //clickByEachUser.print("Total clicks by each User: ");
        //top clicks user
        SingleOutputStreamOperator<Tuple2<String, Long>> topClickUser = clickByEachUser.keyBy(value -> "global")
                        .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                            @Override
                            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                                if (value1.f1 > value2.f1) {
                                    return value1;
                                } else {
                                    return value2;
                                }
                            }
                        });
        topClickUser.print();
        environment.execute();
    }
}
