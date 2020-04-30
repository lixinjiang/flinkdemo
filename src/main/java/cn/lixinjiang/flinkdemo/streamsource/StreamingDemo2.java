package cn.lixinjiang.flinkdemo.streamsource;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

/**
 * RichMapFunction
 *
 * @Author lxj
 */
public class StreamingDemo2 extends BaseStreaming<Item, String> {
    public static void main(String[] args) throws Exception {
        new StreamingDemo2().run();
    }

    @Override
    public DataStream<String> addCondition(DataStreamSource<Item> items) {
        return items.map(new MyMapFunction());
    }
}
