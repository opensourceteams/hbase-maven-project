package com.opensource;


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
public class ScanTest {



    public static final byte[] CF = "cf".getBytes();
    public static final byte[] CF_A = "a".getBytes();
    public static final byte[] TABLE = "test".getBytes();
    public static final java.lang.String TABLE_TEST = "test";
    Connection connection = null;
    Configuration config = null;
    @Before
    public void setUp() throws Exception {

        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "c0,c1,c2");  // Here we are running zookeeper locally
        connection = ConnectionFactory.createConnection(config);
    }
    /**
     * 过滤行key
     * @throws IOException
     */
    @Test
    public void testFilterRowKey() throws IOException {

        Table table = connection.getTable(TableName.valueOf(TABLE_TEST));
        Scan scan = new Scan();
        scan.addColumn(CF,CF_A);
        scan.setRowPrefixFilter(Bytes.toBytes("row"));//过

        ResultScanner rs  = table.getScanner(scan);
        try{
            for(Result result = rs.next();result !=null;result = rs.next()){

                System.out.print( Bytes.toString(result.getRow()));
                System.out.print(Bytes.toString(result.getValue(CF,CF_A)));
                System.out.println();
            }
        }finally {
            rs.close();
        }

    }

    /**
     * 过滤列
     * @throws IOException
     */
    @Test
    public void testScanColumn() throws IOException {
       connection = ConnectionFactory.createConnection(config);
        Table table = connection.getTable(TableName.valueOf(TABLE_TEST));
        Scan scan = new Scan();
        scan.addColumn(CF,CF_A);
        ResultScanner rs  = table.getScanner(scan);
        try{
            for(Result result = rs.next();result !=null;result = rs.next()){
                System.out.print( Bytes.toString(result.getRow()));
                System.out.print(Bytes.toString(result.getValue(CF,CF_A)));
                System.out.println();
            }
        }finally {
            rs.close();
        }

    }


    /**
     * 全表查询
     * @throws IOException
     */
    @Test
    public void testScanAll() throws IOException {
      connection = ConnectionFactory.createConnection(config);

        Table table = connection.getTable(TableName.valueOf(TABLE_TEST));
        Scan scan = new Scan();
        ResultScanner rs  = table.getScanner(scan);
        try{
            for(Result result = rs.next();result !=null;result = rs.next()){

                System.out.print( Bytes.toString(result.getRow()));
                System.out.print(" ");
                System.out.print(Bytes.toString(result.getValue(CF,CF_A)));

                System.out.println();
            }
        }finally {
            rs.close();
        }

    }
}
