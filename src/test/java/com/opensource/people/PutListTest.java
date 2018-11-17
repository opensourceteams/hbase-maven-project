package com.opensource.people;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;


import java.util.ArrayList;
import java.util.List;

/**
 * 开发人:刘文
 * 日期:  2017/6/29.
 * 功能描述:
 */
public class PutListTest {


    Logger logger = Logger.getLogger(PutListTest.class);

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
    public void testPut() throws Exception {

        Table table = connection.getTable(TableName.valueOf(TABLE_TEST));
        Put put = new Put(Bytes.toBytes("r1"));
        put.addColumn(CF,CF_A,Bytes.toBytes(1));
        put.addColumn(CF,CF_B,Bytes.toBytes(2));
        table.put(put);
    }


    @Test
    public void testPutList() throws Exception {

        Table table = connection.getTable(TableName.valueOf(TABLE_TEST));
        List<Put> putList = new ArrayList<>();
        for(int i = 11000 ; i < 12000 ;i++){
            Put put = new Put(Bytes.toBytes("r" + i ));
            put.addColumn(CF,CF_A,Bytes.toBytes(String.valueOf(i)));
            put.addColumn(CF,CF_B,Bytes.toBytes("b" + String.valueOf(i)));
            putList.add(put);
            table.put(putList);
        }

    }


    @Test
    public void testPutList2() throws Exception {

        Table table = connection.getTable(TableName.valueOf(TABLE_TEST));
        List<Put> putList = new ArrayList<>();
        for( int j = 1 ;j<=10; j++){
            logger.info("j:" + j  );
            for(int i = 60000 ; i < 60500 ;i++){
                Put put = new Put(Bytes.toBytes("r" +j + i ));
                put.addColumn(CF,CF_A,Bytes.toBytes(j + String.valueOf(i)));
                put.addColumn(CF,CF_B,Bytes.toBytes("b" + j + String.valueOf(i)));
                putList.add(put);
                table.put(putList);
            }

        }


    }

    @Test
    public void a(){
        logger.info("j:aa"   );
    }
}
