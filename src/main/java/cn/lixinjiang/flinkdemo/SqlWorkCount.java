package cn.lixinjiang.flinkdemo;

import java.util.ArrayList;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * @Author lxj
 */
public class SqlWorkCount {
    public static void main(String[] args) throws Exception {
        /**创建上下文环境*/
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        /**读取一行模拟数据作为输入*/
        String words = "hello flink hello duxiaoman";
        String[] split = words.split("\\W+");
        ArrayList<WC> list = new ArrayList();

        for (String word : split) {
            WC wc = new WC(word, 1L);
            list.add(wc);
        }
        DataSet<WC> input = env.fromCollection(list);

        /**注册成表，执行sql*/
        // DataSet 转sql，指定字段名
        Table table = tableEnv.fromDataSet(input, "word,frequency");
        table.printSchema();

        // 注册为一个表
        tableEnv.createTemporaryView("WordCount", table);
        Table tb2 = tableEnv.sqlQuery("select word as word, sum(frequency) as frequency from WordCount GROUP BY word");

        // 将表转化为DataSet
        DataSet<WC> wcDataSet = tableEnv.toDataSet(tb2, WC.class);
        wcDataSet.printToErr();
    }

    public static class WC {
        public String word;
        public long frequency;

        public WC() {

        }

        public WC(String word, long frequency) {
            this.word = word;
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return word + "," + frequency;
        }
    }

}
