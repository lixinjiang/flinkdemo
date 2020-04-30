package cn.lixinjiang.flinkdemo.tablesql;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import cn.lixinjiang.flinkdemo.streamsource.Item;

/**
 * 将实时商品数据流进行分流，分成even和odd两个流进行JOIN，条件是名称相同，最后，把两个流的JOIN结果输出
 *
 * @Author lxj
 */
public class StreamDemo {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        SingleOutputStreamOperator<Item> source =
                bsEnv.addSource(new TableStreamSource()).map((MapFunction<Item, Item>) item -> item);

        DataStream<Item> evenSelect = getSelectDataSource(source).select("even");

        DataStream<Item> oddSelect = getSelectDataSource(source).select("odd");

        bsTableEnv.createTemporaryView("evenTable", evenSelect, "name,id");
        bsTableEnv.createTemporaryView("oddTable", oddSelect, "name,id");

        Table queryTable = bsTableEnv
                .sqlQuery("select a.id as evenId,a.name as evenName,b.id as oddId,b.name as oddName from evenTable as "
                        + "a join oddTable as b on a.name  = b.name");

        queryTable.printSchema();

        bsTableEnv.toRetractStream(queryTable,
                TypeInformation.of(new TypeHint<Tuple4<Integer, String, Integer, String>>() {
                })).print();

        bsEnv.execute("streaming sql job.");
    }

    private static SplitStream<Item> getSelectDataSource(DataStream<Item> source) {
        return source.split((OutputSelector<Item>) value -> {
            List<String> output = new ArrayList<>();
            if (value.getId() % 2 == 0) {
                output.add("even");
            } else {
                output.add("odd");
            }
            return output;
        });
    }
}
