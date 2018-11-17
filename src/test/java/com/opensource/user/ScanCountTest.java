package com.opensource.user;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.shell.Count;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * 开发人:刘文
 * 日期:  2017/6/29.
 * 功能描述:
 */
public class ScanCountTest {



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
     * 统计表行数
     * @throws IOException
     */
    @Test
    public void testScanCount() throws IOException {

        Table table = connection.getTable(TableName.valueOf(TABLE_TEST));
        Scan scan = new Scan();
        scan.setFilter(new FirstKeyOnlyFilter());

        ResultScanner rs  = table.getScanner(scan);
        final Long[] rowCount = {0l};

        try{
            rs.forEach( result ->  {
                rowCount[0] += result.size() ;} );

        }finally {
            rs.close();
        }

        System.out.println(rowCount[0]);
    }
}
