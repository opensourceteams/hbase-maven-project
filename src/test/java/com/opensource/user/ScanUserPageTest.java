package com.opensource.user;


import org.apache.hadoop.conf.Configuration;
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
public class ScanUserPageTest {



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
        config.set("hbase.zookeeper.quorum", "c0,c1,c2");  // Here we are running zookeeper locally
        connection = ConnectionFactory.createConnection(config);
    }
    /**
     * 查询所有数据
     * @throws IOException
     */
    @Test
    public void testScan() throws IOException {

        Table table = connection.getTable(TableName.valueOf(TABLE_TEST));
        Scan scan = new Scan();
       scan.setStartRow(Bytes.toBytes(11));
        scan.setStopRow(Bytes.toBytes(20));

        ResultScanner rs  = table.getScanner(scan);

        try{
            for(Result result = rs.next();result !=null;result = rs.next()){
                System.out.print( Bytes.toInt(result.getRow())+" ");
                System.out.print(Bytes.toInt(result.getValue(CF,CF_AGE)) +" ");
                System.out.print(Bytes.toString(result.getValue(CF,CF_NAME))+" ");
                System.out.println();
            }
        }finally {
            rs.close();
        }
    }


}
