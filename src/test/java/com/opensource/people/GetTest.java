package com.opensource.people;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

/**
 * 开发人:刘文
 * 日期:  2017/6/29.
 * 功能描述:
 */
public class GetTest {


    Logger logger = Logger.getLogger(GetTest.class);

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
        Get get = new Get( Bytes.toBytes("r1"));
        Result result = table.get(get);
        byte[] c1 = result.getValue(CF,CF_A);
        System.out.println(Bytes.toInt(c1));
        System.out.println(Bytes.toString(c1));
    }
    @Test
    public void testGetNew() throws Exception {
        Table table = connection.getTable(TableName.valueOf(TABLE_TEST));
        Get get = new Get( Bytes.toBytes("r1"));
        Result result = table.get(get);
        for(Cell cell :result.rawCells()){
            logger.info(" family:qualifier   " +Bytes.toString(CellUtil.cloneFamily(cell)) + ":" +Bytes.toString(CellUtil.cloneQualifier(cell)));
            //logger.info(" qualifier:" +Bytes.toString(CellUtil.cloneQualifier(cell)));
            logger.info(" value:" + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println();
        }

        IOUtils.closeStream(table);


    }
}
