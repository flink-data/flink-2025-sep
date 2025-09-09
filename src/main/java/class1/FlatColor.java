package class1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatColor {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStreamSource<String> streamSource = environment.fromElements("red", "white", "green");
        // if color == white, white white white, if color == red, red red if color == green collect nothing
        streamSource.flatMap(new FlatColorMapper()).print();
        environment.execute();
    }

    public static class FlatColorMapper implements FlatMapFunction<String, String> {

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            if (value.equals("red")) {
                out.collect("red");
                out.collect("red");
            } else if (value.equals("white")) {
                out.collect("white");
                out.collect("white");
                out.collect("white");
            }
        }
    }
}
