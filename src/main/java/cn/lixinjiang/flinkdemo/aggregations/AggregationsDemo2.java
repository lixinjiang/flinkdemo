package cn.lixinjiang.flinkdemo.aggregations;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lxj
 */
public class AggregationsDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List data = new ArrayList<Tuple3<Integer, Integer, Integer>>();
        data.add(new Tuple3<>(0, 1, 0));
        data.add(new Tuple3<>(0, 1, 1));
        data.add(new Tuple3<>(0, 2, 2));
        data.add(new Tuple3<>(0, 1, 3));
        data.add(new Tuple3<>(1, 2, 5));
        data.add(new Tuple3<>(1, 2, 9));
        data.add(new Tuple3<>(1, 2, 11));
        data.add(new Tuple3<>(1, 2, 13));
        DataStreamSource<Tuple3<Integer, Integer, Integer>> items = env.fromCollection(data);

        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> reduce =
                items.keyBy(0).reduce((ReduceFunction<Tuple3<Integer, Integer, Integer>>) (t1, t2) -> {
                    Tuple3<Integer, Integer, Integer> newTuple = new Tuple3<>();
                    newTuple.setFields(0, 0, (Integer) t1.getField(2) + (Integer) t2.getField(2));
                    return newTuple;
                });
        reduce.printToErr().setParallelism(1);

        env.execute();
    }
}
