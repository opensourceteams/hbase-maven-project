package com.opensource.user;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

/**
 * 开发人:刘文
 * 日期:  2017/6/29.
 * 功能描述:
 */
public class PutTest {



    public static final byte[] CF = "cf".getBytes();
    public static final byte[] CF_AGE = "age".getBytes();
    public static final byte[] CF_NAME = "name".getBytes();
    public static final byte[] TABLE = "test".getBytes();
    public static final String TABLE_TEST = "user";
    Connection connection = null;
    Configuration config = null;
    @Before
    public void setUp() throws Exception {

        config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.zookeeper.quorum", "c0,c1,c2");  // Here we are running zookeeper locally
        connection = ConnectionFactory.createConnection(config);
    }
    /**
     * 插入单行数据
     * @throws Exception
     */
    @Test
    public void testPut() throws Exception {
        Table table = connection.getTable(TableName.valueOf(TABLE_TEST));
        Put put = new Put(Bytes.toBytes("row3"));
        put.addColumn(CF,CF_AGE,Bytes.toBytes(22));
        put.addColumn(CF,CF_NAME,Bytes.toBytes("中国"));
        table.put(put);
    }

}
