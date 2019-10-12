package com.xiaolong.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * DDL:
 * 1.判断表是否存在
 * 2.创建表
 * 3.创建命名空间
 * 4.删除表
 * DML
 * 5.插入数据
 * 6.查询数据 get scan
 * 7.删除数据
 */
public class TestAPI {

    private static Connection connection = null;
    private static Admin admin = null;

    static {
        try {
            //1.获取配置文件信息
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum","hadoop001,hadoop002,hadoop003");
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    //1.判断表是否存在
    public static boolean isTableExist(String tableName) throws IOException {


        boolean exists = admin.tableExists(TableName.valueOf(tableName));

        return exists;
    }
   //2.创建表
    public static void createTable(String tableName,String... cfs) throws IOException {

        //1.判断是否存在列族信息
        if(cfs.length <=0){
            System.out.println("请设置列族信息");
            return;
        }

        //判断表是否存在
        if(isTableExist(tableName)){
            System.out.println(tableName+"表已经存在");
            return;
        }
        //创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        //循环添加列族信息
        for(String cf:cfs){

            //5.创建列族描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        //创建表
        admin.createTable(hTableDescriptor);
    }
     //删除表
    public static void dropTable(String tableName) throws IOException {
        //1.判断表是否存在
        if(!isTableExist(tableName)){
            System.out.println(tableName+"表不存在!");
        }
        //2.使表下线
        admin.disableTable(TableName.valueOf(tableName));
        //3.删除表
        admin.deleteTable(TableName.valueOf(tableName));
    }
    //4.创建命名空间
    public static void createNameSpace(String ns){
        //1.创建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();
        try {
            admin.createNamespace(namespaceDescriptor);
        }
        catch (NamespaceExistException e){
            System.out.println(ns+"命名空间已经存在!");
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("hahaha,尽管存在我还是走到这里");
    }



    public static void close(){
        if(admin!=null){
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if(connection!=null){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    public static void main(String[] args) throws IOException {
        //1.测试表是否存在
        System.out.println(isTableExist("stu5"));

        //2
//        createTable("ns:stu5","info1","info2");
//
//        System.out.println(isTableExist("stu5"));
         dropTable("stu5");
        System.out.println(isTableExist("stu5"));
        close();
    }
}
