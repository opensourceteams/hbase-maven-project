package com.opensource;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

/**
 * 开发人:刘文
 * 日期:  2017/6/29.
 * 功能描述:
 */
public class GetTest {



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
    public void testGet() throws Exception {
        Table table = connection.getTable(TableName.valueOf(TABLE_TEST));
        Get get = new Get(Bytes.toBytes(1));
        Result result = table.get(get);
        System.out.print( Bytes.toString(result.getRow()));
        System.out.print(Bytes.toString(result.getValue(CF,CF_A)));
        System.out.println();

    }

}
