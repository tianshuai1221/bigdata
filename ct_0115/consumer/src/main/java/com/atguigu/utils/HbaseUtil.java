package com.atguigu.utils;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * 创建命名空间
 * 创建表
 * rowkey设计
 * 分区健的生成
 */
public class HbaseUtil {

    private static Configuration conf = HBaseConfiguration.create();

    //判断表是否存在
    public static boolean isTableExist(String tableName) throws IOException {
        //获取连接
        Connection connection = ConnectionFactory.createConnection(conf);
        //获取admin对象
        Admin admin = connection.getAdmin();
        //返回表是否存在
        boolean result = admin.tableExists(TableName.valueOf(tableName));
        //关闭相关资源
        close(connection, admin);

        return result;
    }

    //初始化命名空间
    public static void initNamespace(String namespace) throws IOException {

        //获取连接
        Connection connection = ConnectionFactory.createConnection(conf);
        //获取admin对象
        Admin admin = connection.getAdmin();

        //创建namespace描述器
        NamespaceDescriptor descriptor = NamespaceDescriptor.create(namespace).build();

        //创建namespace
        try {
            admin.createNamespace(descriptor);
        }catch (Exception e){
            System.out.println(namespace+"已存在");
        }
        //关闭相关资源
        close(connection, admin);
    }

    //创建表
    public static void createTable(String tableName, int regions, String... columnFamily) throws IOException {

        if (isTableExist(tableName)) {
            System.out.println("表" + tableName + "已存在！");
            return;
        }
        //获取连接
        Connection connection = ConnectionFactory.createConnection(conf);
        //获取admin对象
        Admin admin = connection.getAdmin();

        //创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //添加列族
        for (String cf : columnFamily) {
            hTableDescriptor.addFamily(new HColumnDescriptor(cf));
        }

        //添加协处理器
       // hTableDescriptor.addCoprocessor("com.atguigu.coprocessor.CalleeWriteObserver");
//        int regions = Integer.valueOf(PropertyUtil.properties.getProperty("hbase.regions"));
        //创建表（带分区健）
        admin.createTable(hTableDescriptor, getSplitKeys(regions));

        close(connection, admin);
    }

    /**
     * 预分区健
     * {00|，01|，02|，03|...}
     *
     * @param regions 6
     */
    public static byte[][] getSplitKeys(int regions) {

        DecimalFormat df = new DecimalFormat("00");

        byte[][] splitKeys = new byte[regions][];

        for (int i = 0; i < regions; i++) {
            splitKeys[i] = Bytes.toBytes(df.format(i) + "|");
        }

        for (byte[] splitKey : splitKeys) {
            System.out.println(Bytes.toString(splitKey));
        }
        return splitKeys;
    }

    /**
     * 生成rowkey
     * xxx13651234567_2019-02-21 13:13:13_13891234567_0180
     * regionHash_caller_buildTime_callee_duration
     */
    public static String getRowKey(String regionHash,
                                   String caller,
                                   String buildTime,
                                   String callee,
                                   String flag,
                                   String duration) {

        return regionHash + "_" + caller + "_" + buildTime + "_" + callee + "_" + flag + "_" + duration;
    }

    /**
     * 生成分区号
     * 00_,01_,...
     */
    public static String getRegionHash(String caller, String buildTime, int regions) {
        int len = caller.length();
        //获取手机号后4位
        String last4Num = caller.substring(len - 4);

        //获取年月
        String yearMonth = buildTime.replaceAll("-", "").substring(0, 6);
        int regionCode = (Integer.valueOf(last4Num) ^ Integer.valueOf(yearMonth)) % regions;

        DecimalFormat df = new DecimalFormat("00");

        return df.format(regionCode);
    }


    //关闭资源
    private static void close(Connection connection, Admin admin, Table... tables) {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (tables.length <= 0) return;

        for (Table table : tables) {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


    }

}
