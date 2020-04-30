package cn.lixinjiang.flinkdemo.streamsource;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

/**
 * 有向无环图
 *
 * @Author lxj
 */
public class StreamingDemo1 extends BaseStreaming<Item, Item> {
    public static void main(String[] args) throws Exception {
        new StreamingDemo1().run();
    }

    @Override
    public DataStream<Item> addCondition(DataStreamSource<Item> items) {
        return items.map((MapFunction<Item, Item>) value -> value);
    }
}