package class2;

import class1.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichMapFunctionTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<Event> stream = environment.fromElements(
                new Event("/cart", "Tom", 1000L ),
                new Event("/fav", "Tom", 2000L ),
                new Event("/home", "Ben", 3000L ),
                new Event("/order", "Nancy", 4000L ),
                new Event("/close", "Tom", 5000L ),
                new Event("/refund", "Williams", 600L )
        );
        stream.map(new RichMapFunction<Event, Integer>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("Start of life cycle: " + getRuntimeContext().getIndexOfThisSubtask());
            }

            @Override
            public Integer map(Event value) throws Exception {
                return value.user.length();
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("End of life cycle: " + getRuntimeContext().getIndexOfThisSubtask());
            }
        }).print();
        environment.execute();
    }
}
