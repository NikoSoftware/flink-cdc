package org.apache.flink;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.config.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class MysqlCDC {
    public static void main(String[] args) throws Exception {

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("www.xiaomotou.cn")
                .port(3306)
                .startupOptions(StartupOptions.initial()) //全量同步
                .scanNewlyAddedTableEnabled(true) // 开启支持新增表
                .databaseList("hifive_player_db_v2") // set captured database
                .tableList("hifive_player_db_v2.t_weather") // set captured table
                .username("root")
                .password("root@2022")
                .debeziumProperties(getDebeziumProperties())
                 .serverTimeZone("Asia/Shanghai")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        Configuration configuration = new Configuration();
        // 生产环境夏下，改成参数传进来
//        configuration.setString("execution.savepoint.path","file:///tmp/flink-ck/1980d53f557a886f885172bcdf4be8e8/chk-21");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        // enable checkpoint
        env.enableCheckpointing(3000);
        // 设置本地
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-ck");
        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(4)
                .addSink(new MysqlSink());
//                .print("==>").setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute("Print MySQL Snapshot + Binlog");

    }

    private static Properties getDebeziumProperties(){
        Properties properties = new Properties();
        properties.setProperty("converters", "dateConverters");
        //根据类在那个包下面修改
        properties.setProperty("dateConverters.type", "org.apache.flink.config.MySqlDateTimeConverter");
        properties.setProperty("dateConverters.format.date", "yyyy-MM-dd");
        properties.setProperty("dateConverters.format.time", "HH:mm:ss");
        properties.setProperty("dateConverters.format.datetime", "yyyy-MM-dd HH:mm:ss");
        properties.setProperty("dateConverters.format.timestamp", "yyyy-MM-dd HH:mm:ss");
        properties.setProperty("dateConverters.format.timestamp.zone", "UTC+8");
        properties.setProperty("debezium.snapshot.locking.mode","none"); //全局读写锁，可能会影响在线业务，跳过锁设置
        properties.setProperty("include.schema.changes", "true");
        properties.setProperty("bigint.unsigned.handling.mode","long");
        properties.setProperty("decimal.handling.mode","double");
        return properties;
    }

}