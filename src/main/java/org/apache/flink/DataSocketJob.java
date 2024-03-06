package org.apache.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


public class DataSocketJob {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = env.socketTextStream("www.xiaomotou.cn", 8088);


        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = streamSource.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (data, collector) -> {
            String[] s1 = data.split(" ");

            for (String s2 : s1) {
                collector.collect(Tuple2.of(s2, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy((KeySelector<Tuple2<String, Integer>, String>) stringIntegerTuple2 -> stringIntegerTuple2.f0)
                //.countWindow(3, 5)
                .sum(1);

        sum.print();

        env.execute();
    }



}
