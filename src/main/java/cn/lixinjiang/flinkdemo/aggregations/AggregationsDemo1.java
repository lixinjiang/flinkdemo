package cn.lixinjiang.flinkdemo.aggregations;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lxj
 */
public class AggregationsDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List data = new ArrayList<Tuple3<Integer, Integer, Integer>>();
        for (int i = 1; i < 11; i++) {
            data.add(new Tuple3<>(i % 2, i % 5 % 3, i));
        }
        DataStreamSource items = env.fromCollection(data);
        items.keyBy(0).max(2).printToErr();

        env.execute();
    }
}
