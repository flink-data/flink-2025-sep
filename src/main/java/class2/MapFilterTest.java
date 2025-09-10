package class2;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.xml.crypto.Data;

public class MapFilterTest {

    //接受一批User信息
    //Map -> Detail User, 把名字换成大写，加一个tag，如果大于等于18，tag = adult, 如果小于，tag = child
    //Filter -> Return tag == adult的所有用户。
    public static class User {
        String name;
        Integer age;

        public User(Integer age, String name) {
            this.age = age;
            this.name = name;
        }
    }

    public static class DetailUser {
        String name;
        Integer age;
        String tag;

        public DetailUser(String name, Integer age, String tag) {
            this.name = name;
            this.age = age;
            this.tag = tag;
        }

        @Override
        public String toString() {
            return "DetailUser{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    ", tag='" + tag + '\'' +
                    '}';
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<User> stream = environment.fromElements(
                new User(30, "Ben"),
                new User(20, "Bob"),
                new User(3, "Nancy")
        );
        //map

        SingleOutputStreamOperator<DetailUser> userDataStream = stream.map(new MapFunction<User, DetailUser>() {
            @Override
            public DetailUser map(User value) throws Exception {
                String upperName = value.name.toUpperCase();
                String tag = "adult";
                if (value.age < 18) {
                    tag = "child";
                }
                return new DetailUser(value.name, value.age, tag);
            }
        });
        //userDataStream.print();

        //filter
        DataStream<DetailUser> filterdUsers = userDataStream.filter(new FilterFunction<DetailUser>() {
            @Override
            public boolean filter(DetailUser value) throws Exception {
                return value.tag.equals("adult");
            }
        });
        filterdUsers.print();
        environment.execute();

    }
}
