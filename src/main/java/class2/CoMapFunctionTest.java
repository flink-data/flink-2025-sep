package class2;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class CoMapFunctionTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<Integer> streamSource1 = environment.fromElements(1, 2, 3, 4 ,5 ,6);
        DataStreamSource<Long> streamSource2 = environment.fromElements(6L, 7L, 8L, 9L, 10L);
        DataStreamSource<String> streamSource3 = environment.fromElements("mobile_1", "mobile_2", "mobile_3");
        DataStreamSource<String> streamSource4 = environment.fromElements("web_1", "web_2", "web_3");
        DataStream<String> unionStream = streamSource3.union(streamSource4);
        streamSource1.connect(streamSource2).map(new CoMapFunction<Integer, Long, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return "Source 1: " + value.toString();
            }

            @Override
            public String map2(Long value) throws Exception {
                return "Source 2: " + value.toString();
            }
        }).print();
        unionStream.print();
        environment.execute();
    }
}
