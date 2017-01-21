package hbaseApp;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class App
{
    public static void main( String[] args ) throws IOException {
        String zk = "cesvima141G5H2:2181";

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zk);
        conf.set("hbase.zookeeper.property.clientPort", zk);

        HBaseAdmin admin = new HBaseAdmin(conf);
        byte[] TABLE = Bytes.toBytes("Users");
        byte[] CF = Bytes.toBytes("Basic Data");
        HTableDescriptor table = new
                HTableDescriptor(TableName.valueOf(TABLE));
        HColumnDescriptor family = new
                HColumnDescriptor(CF);
        family.setMaxVersions(10); // Default is 3.
        table.addFamily(family);
        admin.createTable(table);


        System.out.println( "Hello World!ds" );
    }
}
