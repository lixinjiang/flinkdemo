package cn.lixinjiang.flinkdemo.tablesql;

import java.util.ArrayList;
import java.util.Random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import cn.lixinjiang.flinkdemo.streamsource.Item;

/**
 * 数据源
 *
 * @Author lxj
 */
public class TableStreamSource implements SourceFunction<Item> {

    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Item> sourceContext) throws Exception {
        while (isRunning) {
            Item item = generateItem();
            sourceContext.collect(item);

            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    private Item generateItem() {
        int i = new Random().nextInt(100);
        ArrayList<String> list = new ArrayList<>();
        list.add("HAT");
        list.add("TIE");
        list.add("SHOE");
        Item item = new Item();
        item.setName(list.get(new Random().nextInt(3)));
        item.setId(i);
        return item;
    }
}
