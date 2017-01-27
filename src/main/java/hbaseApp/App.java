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
    static byte[] TABLE = Bytes.toBytes( "twitterStats6" );
    static byte[] CF = Bytes.toBytes( "hashtags" );
    static byte[] col11 = Bytes.toBytes( "hashtagFirst" );
    static byte[] col12 = Bytes.toBytes( "countFirst" );
    static byte[] col21 = Bytes.toBytes( "hashtagSecond" );
    static byte[] col22 = Bytes.toBytes( "countSecond" );
    static byte[] col31 = Bytes.toBytes( "hashtagThird" );
    static byte[] col32 = Bytes.toBytes( "countThird" );

    public static void main( String[] args ) throws Exception
    {
    	
    	//Check number of arguments
    	if( args.length < 3 )
        	throw new Exception( "Wrong number of parameters" );
    	
    	//Check mode parameter 
        int Mode = -1;
        try
        {
        	Mode = Integer.parseInt( args[ 0 ] );        	
        }
        catch( Exception e )
        {	
        	Mode = -1;	
        }
        if( Mode < 1 && Mode > 4 )
        	throw new Exception( "First argument(Mode) must be 1, 2, 3 or 4" );
        
        //Check zookeeper argument
        List< String > zkAttributes = Arrays.asList( args[ 1 ].split( ":" ) );
        if( zkAttributes.size() != 2 )
        	throw new Exception( "Zookeper parameter must looks like ZKHOST:ZKPORT" );
        
        String zk = zkAttributes.get( 0 );
        String port = zkAttributes.get( 1 );
        
        //Hbase configurations
        Configuration conf = HBaseConfiguration.create();
        conf.set( "hbase.zookeeper.quorum", zk );
        conf.set( "hbase.zookeeper.property.clientPort", port );
    	HBaseAdmin admin = new HBaseAdmin(conf);      
        
        //Create table and insert data
        if( Mode == 4 )
        {
        	//Table descriptor
            HTableDescriptor table_desc = new HTableDescriptor( TableName.valueOf( TABLE ) );
            HColumnDescriptor family = new HColumnDescriptor( CF );
            table_desc.addFamily( family );
            
            //Check if the table already exist
            if ( !admin.tableExists( TABLE ) ) {
                admin.createTable( table_desc );
            }
            
            
            conn = HConnectionManager.createConnection( conf );
            table = new HTable(TableName.valueOf( TABLE ), conn );
            
        	//Read data from input filepath and insert it into the hbase table
        	String inpath = args[ 2 ];
        	readData( inpath );
        }
        //Queries
        else{
        	//Check number of arguments
        	if( ( Mode == 3 && args.length != 6 ) || ( Mode != 3 && args.length != 7 ) )
        		throw new Exception( "Wrong number of parameters" );
        	//Check if the table exist
        	if ( admin.tableExists( TABLE ) == false ) {
                throw new Exception( "No table to consult" );
            }
        	
        	conn = HConnectionManager.createConnection(conf);
            table = new HTable(TableName.valueOf(TABLE),conn);
            
            //Check the N argument
            Integer n = -1;
            try
            {
            	n = Integer.parseInt( args[ 4 ] );        	
            }
            catch( Exception e )
            {	
            	throw new Exception( "The N parameter must be a number" );	
            }
            
            //Different queries depending on the mode
            if( Mode == 3 )
            	query( n, args[ 2 ], args[ 3 ], "all", Mode, args[ 5 ]);
            else
            	query( n, args[ 2 ], args[ 3 ], args[ 5 ], Mode, args[ 6 ] );
        }
    }

    //Perform queries over hbase table and output results
    public static void query( Integer n, String start, String end, String lang, Integer mode, String path ) throws Exception
    {
    	//Start, end key and list of languages
        byte[] startKey = Bytes.toBytes( start );
        byte[] endKey = Bytes.toBytes( end );
        List<String> langs = Arrays.asList( lang.split( "," ) );

        //Read the table with lnaguages, end and start key, and map the count for each word
        Map< String, Map > allLangHash = scan( startKey, endKey, langs, mode );
        
        //Sort data and print results in the output files
        printData( allLangHash, n, start, end, mode, path );
    }

    //Read from Hbase and map the counts
    public static Map scan( byte[] startKey, byte[] endKey, List< String > langs, Integer mode ) throws Exception
    {
        Map< String, Map > allLangHash = new TreeMap< String, Map >();
        
        //Hbase command
        Scan scan = new Scan( startKey,endKey );
        ResultScanner rs = table.getScanner( scan );

        //Create entry for each language
        for( String lang : langs )
        {
            allLangHash.put( lang,  new TreeMap< String, Integer >() );
        }

        //Go through all the results
        Result res = rs.next();
        Map< String, Integer > HashCount;
        String language, hashFirst, hashSecond, hashThird;
        Integer countFirst, countSecond, countThird = 0;
        while ( res != null && !res.isEmpty() )
        {
        	//Read language from the key
            language = Bytes.toString( res.getRow() ).replaceAll( "[0-9]", "" );
            if( mode == 3 )
            	language = "all";

            //Map results if its language belongs to the list 
            if( allLangHash.containsKey( language ) )
            {
            	HashCount = allLangHash.get( language );
            	hashFirst = Bytes.toString( res.getValue( CF, col11 ) );
                countFirst = Integer.parseInt( Bytes.toString( res.getValue( CF, col12 ) ) );
                hashSecond = Bytes.toString( res.getValue( CF,col21 ) );
                countSecond = Integer.parseInt( Bytes.toString( res.getValue( CF, col22 ) ) );
                hashThird = Bytes.toString( res.getValue( CF, col31 ) );
                countThird = Integer.parseInt( Bytes.toString( res.getValue( CF, col32 ) ) );
                
                if( HashCount.containsKey( hashFirst ) )
                    HashCount.put( hashFirst, HashCount.get( hashFirst ) + countFirst );
                else
                    HashCount.put( hashFirst, countFirst );
                
                if( HashCount.containsKey( hashSecond ) )
                    HashCount.put( hashSecond, HashCount.get( hashSecond ) + countSecond );
                else
                    HashCount.put( hashSecond, countSecond );

                if( HashCount.containsKey( hashThird ) )
                    HashCount.put( hashThird, HashCount.get( hashThird ) + countThird );
                else
                    HashCount.put( hashThird, countThird );
            }
            res = rs.next();
        }
        return allLangHash;
    }

    //print output
    public static void printData( Map< String, Map > allLangHash, Integer n, String start, String end, Integer mode, 
    		String path ) throws Exception
    {
    	//Check output path
    	if( !path.endsWith( "/" ) )
        	path = path + "/";
    	
    	String lang;
    	List< String > HashCountList;
    	
    	FileWriter fw = new FileWriter( path + "05_query" + mode.toString() + ".out", true );
        BufferedWriter bw = new BufferedWriter( fw );
        PrintWriter out = new PrintWriter( bw );
        
        for( Map.Entry< String, Map > entry : allLangHash.entrySet() )
        {
            lang = entry.getKey();
            if( mode == 3 )
            	lang = "NoLanguage";
            
            //Order results
            HashCountList = sortByValue( entry.getValue() );
            
            //Write on output file
            for( int i = 0; i < n; i++ )
            {
                if( i >= HashCountList.size() )
                    out.println( lang + "," + ( i + 1 ) + "," + "null" + "," + start + "," + end );
                else
                    out.println( lang + "," + ( i + 1 ) + "," + HashCountList.get( i ) + "," + start + "," + end );
            }
        }

        out.close();
        bw.close();
        fw.close();
    }

    //Read data from input files and insert
    public static void readData( String path ) throws Exception
    {
        File folder = new File( path );
        String line;
        //Check if its a folder
        if( folder.isDirectory() )
        {
        	for( File fileEntry : folder.listFiles() )
        	{
        		//For each entry check the extension and file type
                if( !fileEntry.isDirectory() && fileEntry.getName().endsWith( ".out" ) )
                {
                	//Read file
                    BufferedReader br = new BufferedReader( new FileReader( fileEntry.getAbsolutePath() ) );
                    try
                    {
                        line = br.readLine();
                        while( line != null )
                        {
                            if( line != null || !line.isEmpty() )
                            {
                                String[] parts = line.split( "," );
                                if(parts.length == 8)
                                {
                                     //Insert
                                     Put put = new Put( Bytes.toBytes( parts[ 0 ] + parts[ 1 ] ) );
                                     put.add( CF, col11, Bytes.toBytes( parts[ 2 ] ) );
                                     put.add( CF, col12, Bytes.toBytes( parts[ 3 ] ) );
                                     put.add( CF, col21, Bytes.toBytes( parts[ 4 ] ) );
                                     put.add( CF, col22, Bytes.toBytes( parts[ 5 ] ) );
                                     put.add( CF, col31, Bytes.toBytes( parts[ 6 ] ) );
                                     put.add( CF, col32, Bytes.toBytes( parts[ 7 ] ) );
                                     table.put( put );
                                }
                                line = br.readLine();
                            }

                        }
                    }
                    catch ( IOException e )
                    {
                        e.printStackTrace();
                    }
                    finally
                    {
                        br.close();
                    }
                }

            }
        }
        

    }

    //Sort map based on count and word
    public static < String, V extends Comparable< ? super V > > List< String > sortByValue( Map< String, V > map )
    {
        List< Map.Entry< String, V > > list = new LinkedList< Map.Entry< String, V > >( map.entrySet() );
        Collections.sort( list, new Comparator< Map.Entry< String, V > >()
        {
            public int compare( Map.Entry< String, V > o1, Map.Entry< String, V > o2 )
            {
            	//Return word comparation in case of equal count
                if( ( o2.getValue() ).compareTo( o1.getValue() ) == 0)
                {
                    java.lang.String key1 = ( ( java.lang.String ) o1.getKey() ).toLowerCase();
                    java.lang.String key2 = ( ( java.lang.String ) o2.getKey() ).toLowerCase();
                    if( key2.compareTo( key1 ) == 0 )
                    {
                    	key1 = ( ( java.lang.String ) o1.getKey() );
                    	key2 = ( ( java.lang.String ) o2.getKey() );
                    }
                    return key1.compareTo( key2 );
                }
                //Return count comparation
                else
                    return ( o2.getValue() ).compareTo( o1.getValue() );
            }
        });
        //Return a list
        List< String > listStrings = new ArrayList< String >();
        for( Map.Entry< String, V > entry : list )
        {
            listStrings.add( entry.getKey() );
        }
        return listStrings;
    }
    
}


