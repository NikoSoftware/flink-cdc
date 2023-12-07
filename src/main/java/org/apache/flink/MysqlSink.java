package org.apache.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.jvnet.hk2.annotations.Service;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * @desc：
 * @name：niko
 * @Date：2023/12/7 13:42
 */
@Service
public class MysqlSink extends RichSinkFunction<String> {
    private Connection connection = null;
    Statement sqlExecute;



    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        try {
            if (connection == null) {
                Class.forName("com.mysql.jdbc.Driver");//加载数据库驱动
                connection = DriverManager.getConnection("jdbc:mysql://www.xiaomotou.cn:3306/demo?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=utf8&useSSL=true"
                        , "root", "root@2022");//获取连接
                sqlExecute = connection.createStatement();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void invoke(String value, Context context) {
        try {
            sqlExecute.execute(value);
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("value = " + value);
    }

    @Override
    public void close() throws Exception {
        super.close();

        if (connection != null) {
            connection.close();
        }
        super.close();
    }
}