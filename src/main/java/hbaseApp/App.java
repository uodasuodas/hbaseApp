package hbaseApp;
import java.io.*;

import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class App
{
    static HConnection conn;
    static HTable table;
    static byte[] TABLE = Bytes.toBytes("twitterStats2");
    static byte[] CF = Bytes.toBytes("hashtags");
    static byte[] colLang = Bytes.toBytes("language");
    static byte[] col11 = Bytes.toBytes("hashtagFirst");
    static byte[] col12 = Bytes.toBytes("countFirst");
    static byte[] col21 = Bytes.toBytes("hashtagSecond");
    static byte[] col22 = Bytes.toBytes("countSecond");
    static byte[] col31 = Bytes.toBytes("hashtagThird");
    static byte[] col32 = Bytes.toBytes("countThird");

    public static void main( String[] args ) throws IOException {
        String zk = "cesvima141G5H4";
        String port = "2181";

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zk);
        conf.set("hbase.zookeeper.property.clientPort", port);

        HBaseAdmin admin = new HBaseAdmin(conf);


        HTableDescriptor table_desc = new
                HTableDescriptor(TableName.valueOf(TABLE));
        HColumnDescriptor family = new
                HColumnDescriptor(CF);

        table_desc.addFamily(family);
        if (admin.tableExists(TABLE) == false) {
            admin.createTable(table_desc);
        }

        conn = HConnectionManager.createConnection(conf);
        table = new HTable(TableName.valueOf(TABLE),conn);

        readData("/home/student/data");

        Scan scan = new Scan();
        ResultScanner rs = table.getScanner(scan);
        Result res = rs.next();
        while (res!=null && !res.isEmpty()){
            byte[] cf = Bytes.toBytes("hashtags");
            String key = Bytes.toString(res.getRow());
            String lang = key.substring(key.length() - 2);
            String hashtag = Bytes.toString(res.getValue(cf, col11));
            String hashtagCount = Bytes.toString(res.getValue(cf, col21));
            System.out.println(lang + "---" + hashtag + "," + hashtagCount);
            System.out.println(key);
            res = rs.next();
        }


        System.out.println( "Hello World2!" );
    }

    public static void query(String start, String end, String lang) throws IOException {
        byte[] startKey = Bytes.toBytes(start);
        byte[] endKey = Bytes.toBytes(end);

        Scan scan = new Scan(startKey,endKey);
        ResultScanner rs = table.getScanner(scan);
        Result res = rs.next();
        while (res!=null && !res.isEmpty()){
            String key = Bytes.toString(res.getRow());
            String language = key.substring(key.length() - 2);
            if (language.equals(lang)) {
                
            }
            res = rs.next();
        }
    }

    public static void readData(String path) throws IOException {
        File folder = new File(path);
        for (File fileEntry : folder.listFiles()) {
            if (!fileEntry.isDirectory() && fileEntry.getName().endsWith(".log")) {
                BufferedReader br = new BufferedReader(new FileReader(fileEntry.getAbsolutePath()));
                try {
                    String line = br.readLine();
                    while (line != null) {
                        if (line != null | !line.isEmpty()) {
                            System.out.println(line);
                            String[] parts = line.split(",");
                            String timestamp = parts[0];
                            String lang = parts[1];
                            byte[] key = Bytes.toBytes(timestamp + lang);
                            byte[] hashFirst = Bytes.toBytes(parts[2]);
                            byte[] countFirst = Bytes.toBytes(parts[3]);
                            byte[] hashSecond = Bytes.toBytes(parts[4]);
                            byte[] countSecond = Bytes.toBytes(parts[5]);
                            byte[] hashThird = Bytes.toBytes(parts[6]);
                            byte[] countThird = Bytes.toBytes(parts[7]);
                            System.out.println(timestamp + lang);
                            Put put = new Put(key);
                            //put.add(CF, colLang, Bytes.toBytes(lang));
                            put.add(CF, col11, hashFirst);
                            put.add(CF, col12, countFirst);
                            put.add(CF, col21, hashSecond);
                            put.add(CF, col22, countSecond);
                            put.add(CF, col31, hashThird);
                            put.add(CF, col32, countThird);
                            table.put(put);
                            line = br.readLine();
                        }

                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    br.close();
                }
            }
        }

    }

}
