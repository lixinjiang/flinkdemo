package cn.lixinjiang.flinkdemo.streamsource;

import java.util.Random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 实现一个自定义实时数据源
 *
 * @Author lxj
 */
public class MyStreamingSource implements SourceFunction<Item> {

    private static final long serialVersionUID = -2296248398731366190L;

    private boolean isRunning = true;

    /**
     * 重写run方法产生一个源源不断的数据发送源
     *
     * @param sourceContext
     *
     * @throws Exception
     */
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

        Item item = new Item();
        item.setName("name" + i);
        item.setId(i);
        return item;
    }
}
