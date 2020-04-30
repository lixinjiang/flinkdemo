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

    /**
     * 根据类名、接口名、成员方法及属性等来生成一个64位的哈希字段   默认为 1L
     * 反序列标识，反序列化过程中，如果字节流中的serialVersionUID本本地实体类的serialVersionUID一直，则可进行反序列化，否则不可
     */
    private static final long serialVersionUID = -7516444794187889441L;

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
