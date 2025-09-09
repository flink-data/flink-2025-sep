package class1;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PersonAge {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Person> stream = environment.fromElements(
                new Person("David", 34),
                new Person("Tom", 3),
                new Person("Bob", 19)
        );
        DataStream<Person> adults = stream.filter(new FilterFunction<Person>() {
            @Override
            public boolean filter(Person value) throws Exception {
                return value.age >= 18;
            }
        });
        adults.print();
        environment.execute();
    }
    public static class Person {
        public String name;
        public Integer age;
        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }
        public Person() {
        }
        @Override
        public String toString() {
            return "Person{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }
    }
}


