package cn.lixinjiang.flinkdemo.streamsource;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

/**
 * Filter
 *
 * @Author lxj
 */
public class StreamingDemo4 extends BaseStreaming<Item,Item> {
    public static void main(String[] args) throws Exception {
        new StreamingDemo4().run();
    }

    @Override
    public DataStream<Item> addCondition(DataStreamSource<Item> items) {
        return items.filter((FilterFunction<Item>) item -> item.getId() % 2 == 0);
    }
}
