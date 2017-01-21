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
        byte[] TABLE = Bytes.toBytes("TopHashtags");
        byte[] CF = Bytes.toBytes("hash");
        HTableDescriptor table_desc = new
                HTableDescriptor(TableName.valueOf(TABLE));
        HColumnDescriptor family = new
                HColumnDescriptor(CF);
        family.setMaxVersions(10); // Default is 3.
        table_desc.addFamily(family);
        admin.createTable(table_desc);

        HConnection conn =
                HConnectionManager.createConnection(conf);
        HTable table = new
                HTable(TableName.valueOf(TABLE),conn);

        for (int i = 0; i < 100; i++){
            byte[] key = Bytes.toBytes("145071446500" + Integer.toString(i));
            byte[] value = Bytes.toBytes("Macbook" + Integer.toString(i % 10));
            byte[] column = Bytes.toBytes("hashword");
            Put put = new Put(key);
            put.add(CF,
                    column,
                    value);
            table.put(put);
        }

        Scan scan = new Scan();
        ResultScanner rs = table.getScanner(scan);
        Result res = rs.next();
        while (res!=null && !res.isEmpty()){
            System.out.println(res);
            res = rs.next();
        }

        System.out.println( "Hello World!" );
    }
}
