package com.opensource.commissionbase;


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
public class CreateTableTest {




    public static final String NAMESSSSPACE_VCN = "vcn";
    public static final String TABLE_COMMISSION_BASE = NAMESSSSPACE_VCN +":tb_commission_base";
    Connection connection = null;
    Configuration config = null;
    Admin admin = null;
    @Before
    public void setUp() throws Exception {

        config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.zookeeper.quorum", "c0,c1,c2");  // Here we are running zookeeper locally
        connection = ConnectionFactory.createConnection(config);
        admin =  connection.getAdmin();
    }


    @Test
    public void deleteAndCreateTable() throws Exception{
        //删除表
        if(admin.tableExists(TableName.valueOf(TABLE_COMMISSION_BASE))){
            admin.disableTable(TableName.valueOf(TABLE_COMMISSION_BASE));
            admin.deleteTable(TableName.valueOf(TABLE_COMMISSION_BASE));
        }


        //创建表
        NamespaceDescriptor descriptor = null;

        try{
            descriptor = admin.getNamespaceDescriptor(NAMESSSSPACE_VCN);
        }catch (org.apache.hadoop.hbase.NamespaceNotFoundException e){
            descriptor =NamespaceDescriptor.create(NAMESSSSPACE_VCN).build();
            admin.createNamespace(descriptor);
        }
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_COMMISSION_BASE)) ;
        HColumnDescriptor family = new HColumnDescriptor("c");

       // HColumnDescriptor name = new HColumnDescriptor("name");

        table.addFamily(family);
        //table.addFamily(name);

        admin.createTable(table);

        admin.close();
    }
    /**
     * 删除表
     * @throws Exception
     */
    @Test
    public void deleteTest() throws Exception {
        if(admin.tableExists(TableName.valueOf(TABLE_COMMISSION_BASE))){
            admin.disableTable(TableName.valueOf(TABLE_COMMISSION_BASE));
            admin.deleteTable(TableName.valueOf(TABLE_COMMISSION_BASE));
        }

        admin.close();
    }

    /**
     * 删除表
     * @throws Exception
     */
    @Test
    public void createTable() throws Exception {

        NamespaceDescriptor descriptor = null;

        try{
            descriptor = admin.getNamespaceDescriptor(NAMESSSSPACE_VCN);
        }catch (org.apache.hadoop.hbase.NamespaceNotFoundException e){
            descriptor =NamespaceDescriptor.create(NAMESSSSPACE_VCN).build();
            admin.createNamespace(descriptor);
        }
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_COMMISSION_BASE)) ;
        HColumnDescriptor family = new HColumnDescriptor("cf");

        table.addFamily(family);

        admin.createTable(table);
        admin.close();
    }

}
