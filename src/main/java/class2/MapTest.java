package class2;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<String> stringDataStream = environment.fromElements("a", "b", "c", "d");
        DataStream<String> upperCaseStream = stringDataStream.map(new UpperCaseMapper());
        upperCaseStream.print();
        environment.execute();
    }

    public static class UpperCaseMapper implements MapFunction<String, String> {

        @Override
        public String map(String value) throws Exception {
            return value.toUpperCase();
        }
    }
}
