package hbaseApp;
import java.io.*;
import java.util.*;

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
        String path = "/home/student/data/output/";

        readData("/home/student/data");

        query(5, "1448471400000", "1448472600000", "en", 1, path);
        query(3, "1448471400000", "1448475600000", "en,fr", 2, path);
        query(6, "1448471400000", "1448475600000", "all", 3, path);
    }

    public static void query(Integer n, String start, String end, String lang, Integer mode, String path)
            throws IOException {
        byte[] startKey = Bytes.toBytes(start);
        byte[] endKey = Bytes.toBytes(end);
        List<String> langs = new ArrayList<String>();
        if(mode == 1 | mode == 2){
            langs = Arrays.asList(lang.split(","));
        }

        Map<String, Map> allLangHash = scan(startKey, endKey, langs, mode);

        printData(allLangHash, n, start, end, mode, path);

    }

    public static Map scan(byte[] startKey, byte[] endKey, List<String> langs, Integer mode) throws IOException {
        Map<String, Map> allLangHash = new TreeMap<String, Map>();
        Scan scan = new Scan(startKey,endKey);
        ResultScanner rs = table.getScanner(scan);

        if (mode == 3) {
            Map< String, Integer > HashCount = new TreeMap< String, Integer >();
            allLangHash.put("all", HashCount);
        } else {
            for (String lang: langs) {
                Map< String, Integer > HashCount = new TreeMap< String, Integer >();
                allLangHash.put(lang, HashCount);
            }
        }

        Map<String, Integer> HashCount;

        Result res = rs.next();
        while (res!=null && !res.isEmpty()){
            String key = Bytes.toString(res.getRow());
            String language = key.substring(key.length() - 2);

            if (mode == 3) {
                language= "all";
            }

            if (allLangHash.containsKey(language)) {
                HashCount = allLangHash.get(language);
                String hashFirst = Bytes.toString(res.getValue(CF,col11));
                Integer countFirst = Integer.parseInt(Bytes.toString(res.getValue(CF,col12)));
                String hashSecond = Bytes.toString(res.getValue(CF,col21));
                Integer countSecond = Integer.parseInt(Bytes.toString(res.getValue(CF,col22)));
                String hashThird = Bytes.toString(res.getValue(CF,col31));
                Integer countThird = Integer.parseInt(Bytes.toString(res.getValue(CF,col32)));
                if (HashCount.containsKey(hashFirst)){
                    Integer count = HashCount.get(hashFirst) + countFirst;
                    HashCount.put(hashFirst, count);
                } else {
                    HashCount.put(hashFirst, countFirst);
                }

                if (HashCount.containsKey(hashSecond)){
                    Integer count = HashCount.get(hashSecond) + countSecond;
                    HashCount.put(hashSecond, count);
                } else {
                    HashCount.put(hashSecond, countSecond);
                }

                if (HashCount.containsKey(hashThird)){
                    Integer count = HashCount.get(hashThird) + countThird;
                    HashCount.put(hashThird, count);
                } else {
                    HashCount.put(hashThird, countThird);
                }
            }

            res = rs.next();
        }
        return allLangHash;
    }

    public static void printData(Map<String,Map> allLangHash, Integer n,
                                 String start, String end, Integer mode, String path) throws IOException {
        for(Map.Entry<String, Map> entry : allLangHash.entrySet()){
            String lang = entry.getKey();
            Map<String, Integer> HashCount = entry.getValue();
            List<String> HashCountList = sortByValue(HashCount);

            FileWriter fw = new FileWriter( path + "05_query" + mode.toString() + ".out", true);
            BufferedWriter bw = new BufferedWriter( fw );
            PrintWriter out = new PrintWriter( bw );
            for (Integer i = 0; i < n; i ++) {
                if (i >= HashCountList.size()){
                    out.println(lang + "," + (i+1) + "," + "null" + "," + start + "," + end);
                } else {
                    String hash = HashCountList.get(i);
                    out.println(lang + "," + (i+1) + "," + hash + "," + start + "," + end);
                }
            }
            out.close();
            bw.close();
            fw.close();
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

    public static <String, V extends Comparable<? super V>> List<String> sortByValue( Map<String, V> map ) {
        List<Map.Entry<String, V>> list = new LinkedList<Map.Entry<String, V>>( map.entrySet() );
        Collections.sort( list, new Comparator<Map.Entry<String, V>>() {
            public int compare( Map.Entry<String, V> o1, Map.Entry<String, V> o2 ) {
                if((o2.getValue()).compareTo( o1.getValue() )==0) {
                    java.lang.String key = ((java.lang.String) o2.getKey()).toLowerCase();
                    java.lang.String key2 = ((java.lang.String) o1.getKey()).toLowerCase();
                    return key2.compareTo(key);
                } else {
                    return (o2.getValue()).compareTo( o1.getValue() );
                }
            }
        });
        List<String> listStrings = new ArrayList<String>();
        for (Map.Entry<String, V> entry : list) {
            listStrings.add(entry.getKey());
        }
        return listStrings;
    }
}


