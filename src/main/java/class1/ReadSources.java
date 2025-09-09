package class1;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class ReadSources {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<String> dataStream1 = environment.fromElements("white", "red", "gray");
        DataStream<Integer> dataStream2 = environment.fromElements(1, 2, 3, 4, 5);
        DataStream<String> dataStream3 = environment.fromCollection(Arrays.asList("white", "red", "gray"));
        DataStreamSource<Long> dataStream4 = environment.generateSequence(1, 5);

        //dataStream1.print();
        //dataStream2.print();
        //dataStream3.print();
        //dataStream4.print();

        DataStreamSource<Event> dataStreamSource = environment.addSource(new ClickSource());
        dataStreamSource.print();
        environment.execute();
    }
}
