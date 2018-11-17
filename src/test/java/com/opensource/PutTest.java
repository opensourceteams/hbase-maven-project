package com.opensource;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * 开发人:刘文
 * 日期:  2017/6/29.
 * 功能描述:
 */
public class PutTest {



    public static final byte[] CF = "cf".getBytes();
    public static final byte[] CF_A = "a".getBytes();
    public static final byte[] CF_B = "b".getBytes();
    public static final byte[] TABLE = "test".getBytes();
    public static final String TABLE_TEST = "test";
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
        Put put = new Put(Bytes.toBytes("row6"));
        put.addColumn(CF,CF_A,Bytes.toBytes(11));
        put.addColumn(CF,CF_B,Bytes.toBytes(11));
        table.put(put);

    }

}
