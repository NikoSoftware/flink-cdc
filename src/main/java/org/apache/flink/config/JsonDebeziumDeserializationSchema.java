package org.apache.flink.config;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * @desc：
 * @name：niko
 * @Date：2023/12/7 13:42
 */

public class JsonDebeziumDeserializationSchema implements DebeziumDeserializationSchema {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector collector) throws Exception {

        //从sourceRecord中获取binlog日志的database，table，type，after，before等数据
        String topic = sourceRecord.topic();
        String[] split = topic.split("[.]");
        String database = split[1];
        String table = split[2];
        //获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        //获取数据本身
        Struct struct = (Struct) sourceRecord.value();
        Struct after = struct.getStruct("after");
        Struct before = struct.getStruct("before");

        //用于拼接所有insert的字段名
        String fieldNames = "";
        //用于拼接所有insert的数据
        String datas = "";
        //用于拼接所有更新的数据
        String updataDatas = "";
        //用于拼接所有更新的where条件
        String updataWhereData = "";
        //获取每张表的唯一主键，用于删除数据
        String key = "id";
        //用于获取每张表唯一主键下所对应的数据
        String key_data = "";

        /*
         	 1，同时存在 beforeStruct 跟 afterStruct数据的话，就代表是update的数据
             2,只存在 beforeStruct 就是delete数据
             3，只存在 afterStruct数据 就是insert数据
        */
        if (after != null && before == null) {
            //拼接insert的sql
            Schema schema = after.schema();
            for (Field field : schema.fields()) {
                Object data = after.get(field);
                String fieldName = field.name();
                String fieldSchema = field.schema().toString();
                if (data != null && !("".equals(data.toString()))) {
                    datas += "'" + data + "',";
                } else {
                    datas +=  "null,";
                }
                fieldNames += fieldName + ",";
            }
        } else if (before != null && after == null) {
            //拼接delete的sql
            Schema schema = before.schema();
            for (Field field : schema.fields()) {
                Object data = before.get(field);
                String fieldName = field.name();
                String fieldSchema = field.schema().toString();

                if (MysqlUtil.isNumType(fieldSchema)) {
                    if (key.equals(fieldName)) {
                        key_data = data.toString();
                    }
                } else {
                    if (key.equals(fieldName)) {
                        key_data = "'" + data.toString() + "'";
                    }
                }
            }
        } else if (before != null && after != null) {
            //拼接update更新后的sql
            Schema schema = after.schema();
            for (Field field : schema.fields()) {

                Object data = after.get(field);
                String fieldName = field.name();
                String fieldSchema = field.schema().toString();
                if (data != null && !("".equals(data.toString()))) {
                    data = data.toString();
                } else {
                    data = "null";
                }
                if (MysqlUtil.isNumType(fieldSchema) || "null".equals(data.toString())) {
                    updataDatas += fieldName + "=" + data + ",";
                } else {
                    updataDatas += fieldName + "=" + "'" + data + "',";
                }
            }
            //拼接update更新前的sql
            Schema before_schema = before.schema();
            for (Field field : before_schema.fields()) {
                Object data = before.get(field);
                String fieldName = field.name();
                String fieldSchema = field.schema().toString();
                if (MysqlUtil.isNumType(fieldSchema)) {
                    if (key.equals(fieldName)) {
                        key_data = data.toString();
                    }
                } else {
                    if (key.equals(fieldName)) {
                        key_data = "'" + data.toString() + "'";
                    }
                }
            }
        }

        //获取gp库的表名
        String gpTableName = table;

        //按数据更新类型拼接最终的sql
        String sql = "";
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)||"read".equals(type)) {

            sql = "insert into " + gpTableName + "(" + fieldNames.substring(0, (fieldNames.length() - 1)) + ") values(" + datas.substring(0, datas.length() - 1) + ");";

        } else if ("delete".equals(type)) {

            sql = "delete from " + gpTableName + " where " + key + "=" + key_data + ";";

        } else if ("update".equals(type)) {

            sql = "update " + gpTableName + " set " + updataDatas.substring(0, updataDatas.length() - 1) + " where " + key + "=" + key_data + ";";

        }

        //如果sql有效，就正常写入gp库，如果sql无效就写入gp错误日志库
        if ((!("".equals(fieldNames) && !("".equals(datas)))) || (!("".equals(key) && !("".equals(key_data)))) || (!("".equals(updataDatas) && !("".equals(updataWhereData))))) {
            collector.collect(sql);
        } else {
            collector.collect("(log_data)" + " values" + "('" + "gpTableName=" + gpTableName + ",type=" + type + ",before=" + before + ",after=" + after + "');");
        }
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

}

