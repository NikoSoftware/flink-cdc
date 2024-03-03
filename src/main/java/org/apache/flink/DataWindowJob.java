package org.apache.flink;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.model.WaterModel;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.format.DateTimeFormatter;

public class DataWindowJob {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<WaterModel> streamSource = env.socketTextStream("www.xiaomotou.cn", 8088)
                .map(new MapFunction<String, WaterModel>() {
                    @Override
                    public WaterModel map(String s) throws Exception {
                        String[] s1 = s.split(" ");

                        WaterModel w = new WaterModel(s1[0], Integer.parseInt(s1[1]), Integer.parseInt(s1[2]));

                        return w;
                    }
                });


        KeyedStream<WaterModel, String> waterModelStringKeyedStream = streamSource.keyBy(new KeySelector<WaterModel, String>() {
            @Override
            public String getKey(WaterModel waterModel) throws Exception {
                return waterModel.name;
            }
        });


        SingleOutputStreamOperator<String> aggregate = waterModelStringKeyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<WaterModel, Integer, String>() {
                               @Override
                               public Integer createAccumulator() {
                                   return 0;
                               }

                               @Override
                               public Integer add(WaterModel waterModel, Integer integer) {
                                   return waterModel.tem + integer;
                               }

                               @Override
                               public String getResult(Integer integer) {
                                   return integer.toString();
                               }

                               @Override
                               public Integer merge(Integer integer, Integer acc1) {
                                   return null;
                               }
                           },new Proc()
                );

        aggregate.print();

        System.out.println("执行");

        env.execute();

    }

    static class Proc extends  ProcessWindowFunction<String, String, String, TimeWindow>{

        @Override
        public void process(String s, ProcessWindowFunction<String, String, String, TimeWindow>.Context context, Iterable<String> iterable, Collector<String> collector) throws Exception {

            long start = context.window().getStart();

            long end = context.window().getEnd();

            String startTime = DateFormatUtils.format(start, "yyyy-MM-dd,HH:mm:ss");

            String endTime = DateFormatUtils.format(end, "yyyy-MM-dd,HH:mm:ss");


            collector.collect("开始时间："+startTime+" 结束时间："+endTime+"结果："+iterable.toString());


        }

    };



}
