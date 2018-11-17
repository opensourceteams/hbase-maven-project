package com.opensource.people;


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
public class ScanTest {



    public static final byte[] CF = "f1".getBytes();
    public static final byte[] CF_A = "c1".getBytes();
    public static final byte[] CF_B = "c2".getBytes();
    public static final String TABLE_TEST = "test:t11";
    Connection connection = null;
    Configuration config = null;
    @Before
    public void setUp() throws Exception {

        config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.zookeeper.quorum", "c2.com");  // Here we are running zookeeper locally
        connection = ConnectionFactory.createConnection(config);
    }
    /**
     * 插入单行数据
     * @throws Exception
     */
    @Test
    public void testGet() throws Exception {
        Table table = connection.getTable(TableName.valueOf(TABLE_TEST));
        Scan scan = new Scan();
        scan.addColumn(CF,CF_A);


        //scan.setAttribute(Scan.HINT_LOOKAHEAD, Bytes.toBytes(2));

        ResultScanner rs  = table.getScanner(scan);
        try{
            for(Result result = rs.next();result !=null;result = rs.next()){
                System.out.println( Bytes.toString(result.getRow()));
                System.out.println(Bytes.toInt(result.getValue(CF,CF_A)));
                System.out.println();
            }
        }finally {
            rs.close();
        }
    }

}
