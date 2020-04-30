package cn.lixinjiang.flinkdemo.streamsource;

import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * 自定义MapFunction
 *
 * @Author lxj
 */
public class MyMapFunction extends RichMapFunction<Item, String> {

    @Override
    public String map(Item item) throws Exception {
        return item.getName();
    }
}
