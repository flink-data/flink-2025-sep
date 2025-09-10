package class2;

import class1.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

public class SinkToFileTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(4);
        DataStream<Event> stream = environment.fromElements(
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
        StreamingFileSink<String> streamingFileSink = StreamingFileSink.<String>forRowFormat(
                new Path("C:\\Users\\admin\\IdeaProjects\\flink-2025-sep\\src\\main\\resources"), new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withMaxPartSize(1024*1024*1024).build()
                ).build();
        stream.map(data -> data.toString()).addSink(streamingFileSink);
        environment.execute();
    }
}
