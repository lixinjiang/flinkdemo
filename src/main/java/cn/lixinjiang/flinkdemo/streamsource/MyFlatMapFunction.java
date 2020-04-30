package cn.lixinjiang.flinkdemo.streamsource;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Flap 将列表平铺
 *
 * @Author lxj
 */
public class MyFlatMapFunction implements FlatMapFunction<Item, Object> {

    @Override
    public void flatMap(Item item, Collector<Object> collector) throws Exception {
        String name = item.getName();
        collector.collect(name);
    }
}
