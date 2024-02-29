package org.apache.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.crypto.KeySelector;


/**
 * @desc：
 * @name：niko
 * @Date：2023/12/7 17:19
 */
public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        // 获取 flink 环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  //      ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


//        DataSource<String> streamSource = env.readTextFile("src/main/resources/test.txt", "UTF-8");
//
//
//        streamSource.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public void flatMap(String s, Collector<String> collector) throws Exception {
//
//            }
//        });


        // 添加数据源
        DataStreamSource<String> streamSource = env.fromElements("hello world", "hello flink", "flink", "hello", "world");
        // 对传入的流数据分组
        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            // value 传入的数据，out
            // Tuple2 二元组
            // out 传出的值
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(" ");
                for (String s : split) {
                    out.collect(Tuple2.of(s, 1));
                }
            }
        });
        // 按二元组的第 0 个位置分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyBy = streamOperator.keyBy(0);
        // 按二元组的第 1 个位置求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyBy.sum(1);
        sum.print();
        env.execute("统计单词出现的次数");
    }
}

