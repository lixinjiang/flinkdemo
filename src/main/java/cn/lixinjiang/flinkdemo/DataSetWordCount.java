package cn.lixinjiang.flinkdemo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author lxj
 */
public class DataSetWordCount {
    public static void main(String[] args) throws Exception {
        // 创建flink运行的上下文环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 创建DataSet，这里我们的输入是一行一行的文本
        DataSet<String> text = env.fromElements("Flink Spark Storm", "Flink Flink Flink", "Spark Storm Strom", "Spark "
                + "Flink Storm Flink");
        // 通过Flink内置的转换函数进行计算
        DataSet<Tuple2<String, Integer>> counts = text.flatMap(new LineSplitter()).groupBy(0).sum(1);
        // 结果打印
        counts.printToErr();
    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        private static final long serialVersionUID = -2046505820215024718L;

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 文本分隔
            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
