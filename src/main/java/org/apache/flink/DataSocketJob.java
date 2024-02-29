package org.apache.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class DataSocketJob {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = env.socketTextStream("www.xiaomotou.cn", 8088);


        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String data, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] s1 = data.split(" ");

                for (String s2 : s1) {
                    collector.collect(Tuple2.of(s2, 1));
                }
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        }).sum(1);

        sum.print();

        env.execute();
    }



}
