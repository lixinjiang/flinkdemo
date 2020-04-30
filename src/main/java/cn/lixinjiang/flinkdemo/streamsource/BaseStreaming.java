package cn.lixinjiang.flinkdemo.streamsource;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * T: 入参
 * V: 返参
 *
 * @Author lxj
 */
public abstract class BaseStreaming<T, V> {
    private String jobName = "user defined streaming source";

    void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取数据源
        DataStreamSource<T> items = (DataStreamSource<T>) env.addSource(new MyStreamingSource()).setParallelism(1);
        DataStream<V> stream = addCondition(items);
        stream.print().setParallelism(1);
        env.execute(jobName);
    }

    public abstract DataStream<V> addCondition(DataStreamSource<T> items);
}
