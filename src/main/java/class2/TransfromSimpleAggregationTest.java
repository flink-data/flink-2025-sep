package class2;

import class1.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransfromSimpleAggregationTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStreamSource<Event> stream = environment.fromElements(
                new Event("/cart", "Tom", 1000L ),
                new Event("/fav", "Tom", 2000L ),
                new Event("/home", "Ben", 3000L ),
                new Event("/order", "Tom", 4000L ),
                new Event("/close", "Tom", 5000L ),
                new Event("/refund", "Tom", 600L )
        );
        stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event value) throws Exception {
                return value.user;
            }
        }).minBy("timestamp").print();
        environment.execute();
    }
}
