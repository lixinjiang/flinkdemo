package cn.lixinjiang.flinkdemo.streamsource;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

/**
 * FlatMap
 *
 * @Author lxj
 */
public class StreamingDemo3 extends BaseStreaming<Item, Object> {
    public static void main(String[] args) throws Exception {
        new StreamingDemo3().run();
    }

    @Override
    public DataStream<Object> addCondition(DataStreamSource<Item> items) {
        return items.flatMap(new MyFlatMapFunction());
    }
}
