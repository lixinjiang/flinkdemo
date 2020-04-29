package cn.lixinjiang.flinkdemo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * nc -lk 9000
 *
 * flink 支持java8 lambda 不友好
 *
 * @Author lxj
 */
public class StreamingWordCount {
    public static void main(String[] args) throws Exception {
        // 创建Flink的流式计算环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 监听本地9000端口
        DataStreamSource<String> text = env.socketTextStream("127.0.0.1", 9000, "\n");

        // 将接收的数据进行拆分
        SingleOutputStreamOperator<WordWithCount> windowsCounts =
                text.flatMap(new FlatMapFunction<String, WordWithCount>() {
                    @Override
                    public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                        for (String word : value.split("\\s")) {
                            out.collect(new WordWithCount(word, 1L));
                        }
                    }
                }).keyBy("word").timeWindow(Time.seconds(5), Time.seconds(1))
                        .reduce(new ReduceFunction<WordWithCount>() {
                            @Override
                            public WordWithCount reduce(WordWithCount a, WordWithCount b) throws Exception {
                                return new WordWithCount(a.word, a.count + b.count);
                            }
                        });

        windowsCounts.print().setParallelism(1);
        env.execute("Socket window WordCount");
    }

    public static class WordWithCount {
        public String word;
        public long count;

        public WordWithCount() {

        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return this.word + "：" + this.count;
        }
    }
}
