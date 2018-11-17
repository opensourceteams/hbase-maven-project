package com.opensource.people;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.junit.Before;
import org.junit.Test;

/**
 * 开发人:刘文
 * 日期:  2017/6/29.
 * 功能描述:
 */
public class CreateTest {

    HBaseAdmin  admin = null;
    Configuration config = null;
    Connection connection = null;
    @Before
    public void setUp() throws Exception {

        config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.zookeeper.quorum", "c2.com");  // Here we are running zookeeper locally
        connection = ConnectionFactory.createConnection(config);
        admin = (HBaseAdmin) connection.getAdmin();
    }
    /**
     * 创建表
     * @throws Exception
     */
    @Test
    public void testCreate() throws Exception {

        HTableDescriptor tableDesc = new HTableDescriptor("test:t11");
        HColumnDescriptor cfDesc = new HColumnDescriptor("f1");
        cfDesc.setPrefetchBlocksOnOpen(true);
        tableDesc.addFamily(cfDesc);
        admin.createTable(tableDesc);
    }

}
