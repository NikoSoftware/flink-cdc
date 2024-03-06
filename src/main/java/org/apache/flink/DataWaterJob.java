package org.apache.flink;

import com.alibaba.fastjson.JSON;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.model.WaterModel;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @desc：
 * @name：niko
 * @Date：2024/3/6 10:03
 */
public class DataWaterJob {

    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<WaterModel> streamSource = env.socketTextStream("www.xiaomotou.cn", 8088)
                .map(new MapFunction<String, WaterModel>() {
                    @Override
                    public WaterModel map(String s) throws Exception {
                        String[] s1 = s.split(" ");

                        WaterModel w = new WaterModel(s1[0], Integer.parseInt(s1[1]), Integer.parseInt(s1[2]),Integer.parseInt(s1[3]));

                        System.out.println(JSON.toJSONString(w));
                        return w;
                    }
                });
        streamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterModel>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterModel>() {
                            @Override
                            public long extractTimestamp(WaterModel waterModel, long l) {
                                return waterModel.getTime();
                            }
                        }));


        KeyedStream<WaterModel, String> waterModelStringKeyedStream = streamSource.keyBy(new KeySelector<WaterModel, String>() {
            @Override
            public String getKey(WaterModel waterModel) throws Exception {
                return waterModel.name;
            }
        });


        SingleOutputStreamOperator<String> process = waterModelStringKeyedStream.process(new KeyedProcessFunction<String, WaterModel, String>() {

            ValueState<Integer> valueState;


            @Override
            public void processElement(WaterModel waterModel, KeyedProcessFunction<String, WaterModel, String>.Context context, Collector<String> collector) throws Exception {

            Integer last = valueState.value()==null?0:valueState.value();


            if(Math.abs(waterModel.hum-last)>10){


                collector.collect("当前key:"+context.getCurrentKey()+"      当前水位值:"+waterModel.hum);
            }


            valueState.update(waterModel.hum);

            }



            @Override
            public void open(Configuration parameters) throws Exception {

                valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("valueState", Integer.class));

            }

        });


        process.print();


        env.execute();

    }

}
