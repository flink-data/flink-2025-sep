package class2;

import class1.WordCount;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class TableTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useAnyPlanner().inStreamingMode().build();
        StreamTableEnvironment table = StreamTableEnvironment.create(environment, settings);
        DataStream dataStream = environment.readTextFile("C:\\Users\\admin\\IdeaProjects\\flink-2025-sep\\src\\main\\resources\\word.txt");
        DataStream<Tuple2<String, Integer>> sum = dataStream.flatMap(new WordCount.MyFlatMapper()).keyBy(0).sum(1);
        table.createTemporaryView("test", sum, $("word"), $("count1"));
        Table table1 = table.from("test");
        Table select1 = table1.select($("word"), $("count1")).filter($("count1").isGreater(1));
        Table select2 = table.sqlQuery("select * from test where count1 > 1");
        table.toRetractStream(select1, Row.class).print();
        table.toRetractStream(select2, Row.class).print();
        environment.execute();
    }
}
