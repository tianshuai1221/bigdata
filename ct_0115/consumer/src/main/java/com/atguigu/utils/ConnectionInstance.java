package com.atguigu.utils;

import com.atguigu.constants.Constant;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * 获取连接的单例
 */
public class ConnectionInstance {

    private static Connection connection;

    //私有化构造器
    private ConnectionInstance() {
    }

    public static  Connection getInstance(){
        if (connection == null || connection.isClosed()){
            try {
                connection = ConnectionFactory.createConnection(Constant.CONF);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }
}
