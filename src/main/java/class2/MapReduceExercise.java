package class2;

import class1.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashSet;
import java.util.Set;

public class MapReduceExercise {
    public static void main(String[] args) throws Exception {

        //1. Reduce and print each user's unique URL
        //2. Global user print max number of URL visited
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
                new Event("/order", "Nancy", 4000L )
        );
        //1
        SingleOutputStreamOperator<UserUrlSet> userUrls = stream.map(new MapFunction<Event, UserUrlSet>() {
            @Override
            public UserUrlSet map(Event value) throws Exception {
                Set<String> set = new HashSet<>();
                set.add(value.url);
                return new UserUrlSet(value.user, set);
            }
        });

        SingleOutputStreamOperator<UserUrlSet> urlPerUser = userUrls.keyBy(value -> value.user)
                .reduce(new ReduceFunction<UserUrlSet>() {
                    @Override
                    public UserUrlSet reduce(UserUrlSet value1, UserUrlSet value2) throws Exception {
                        value1.urlSet.addAll(value2.urlSet);
                        return value1;
                    }
                });
        //urlPerUser.print();

        SingleOutputStreamOperator<UserUrlSet> topCount = urlPerUser.keyBy(value -> "global").reduce(new ReduceFunction<UserUrlSet>() {
            @Override
            public UserUrlSet reduce(UserUrlSet value1, UserUrlSet value2) throws Exception {
                if (value1.urlSet.size() > value2.urlSet.size()) {
                    return value1;
                } else {
                    return value2;
                }
            }
        });
        topCount.print();
        environment.execute();
    }

}
