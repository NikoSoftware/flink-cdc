package org.apache.flink.config;

/**
 * @desc：
 * @name：niko
 * @Date：2023/12/7 13:42
 */
public class MysqlUtil {


    /**
     * 传入一个mysql的数据类型，判断该数据类型是否是数字型的
     * @param schema
     * @return
     */
    public static Boolean isNumType(String schema) {
        String[] mysqlNumTypeArr = new String[]{
                "TINYINT",
                "MEDIUMINT",
                "INT",
                "INTEGER",
                "BIGINT",
                "FLOAT",
                "DOUBLE",
                "DECIMAL",
                "LONG"
        };
        Boolean flag = false;
        for (String mysqlNumType : mysqlNumTypeArr) {
            if (schema.toUpperCase().contains(mysqlNumType)) {
                flag = true;
                break;
            }
        }
        return flag;
    }

    /**
     * 传入一个表名，获得该表名的唯一主键key
     * @param table
     * @return
     */
//    public static String getMysqlTableKey(String table) {
//        String[] table_keyArr = SystemConstants.getCdc_table_key();
//
//        String key = "";
//        for (String table_key : table_keyArr) {
//            String[] split = table_key.split(":");
//            if (split[0].equals(table)){
//                key = split[1];
//            }
//        }
//        return key;
//    }


}

