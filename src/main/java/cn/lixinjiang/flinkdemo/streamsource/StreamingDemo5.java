package cn.lixinjiang.flinkdemo.streamsource;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import cn.lixinjiang.flinkdemo.WordWithCount;

/**
 * KeyBy
 *
 * @Author lxj
 */
public class StreamingDemo5 extends BaseStreaming<Item, WordWithCount> {

    public static void main(String[] args) throws Exception {
        new StreamingDemo5().run();
    }

    /**
     * 不知道为啥，这里替换lambda表达式报错
     *
     * @param items
     *
     * @return
     */
    @Override
    public DataStream<WordWithCount> addCondition(DataStreamSource<Item> items) {
        return items.flatMap(new FlatMapFunction<Item, WordWithCount>() {
            @Override
            public void flatMap(Item item, Collector<WordWithCount> collector) throws Exception {
                collector.collect(new WordWithCount(item.getName(), 1L));
            }
        }).keyBy("word").timeWindow(Time.seconds(5), Time.seconds(1)).reduce(new ReduceFunction<WordWithCount>() {
            @Override
            public WordWithCount reduce(WordWithCount a, WordWithCount b) throws Exception {
                return new WordWithCount(a.word, a.count + b.count);
            }
        });
    }
}
