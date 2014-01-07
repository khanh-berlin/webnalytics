package de.dknguyen.lambda;

import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import me.prettyprint.cassandra.model.CqlQuery;
import me.prettyprint.cassandra.model.CqlRows;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthorizationException;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.backtype.support.Utils;

import elephantdb.generated.DomainNotFoundException;
import elephantdb.generated.DomainNotLoadedException;
import elephantdb.generated.HostsDownException;
import elephantdb.generated.Value;
import elephantdb.generated.keyval.ElephantDB;


public class ServingLayer {
	
	
	public static void main(String[] args) throws NumberFormatException, DomainNotFoundException, HostsDownException, DomainNotLoadedException, TException, ParseException, IOException{
		
		String[] cassandraURI = args[0].split(":");
		String[] elephantURI = args[1].split(":");
		String pageid=args[2];
		String dateStart=args[3];
		String dateEnd=args[4];
	
		int count=countPageviews(cassandraURI[0],Integer.valueOf(cassandraURI[1]), 
				elephantURI[0],Integer.valueOf(elephantURI[1]),
				"webanalytics",
				"pageviews",
				pageid, dateStart, dateEnd,"countpageviews");
		
		System.out.println("Views:" + count);
	}
	
	public static int countPageviews(String cassandraHost, 
			int cassandraPort, 
			String elephantdbHost, 
			int elephantdbPort,
			String dbname,
			String elephantdbDomain_cassandraColumnFamily,
			String pageid, 
			String dateStart,
			String dateEnd,
			String cmd) throws DomainNotFoundException, HostsDownException, DomainNotLoadedException, TException, ParseException, IOException{
		
		SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy|hh");
		Date date_start = sdf.parse(dateStart);
		Date date_end = sdf.parse(dateEnd);
		Long timestamp_start=date_start.getTime();
		Long timestamp_end=date_end.getTime();
		int hourStart = (int)(timestamp_start/(3600*1000));
		int hourEnd = (int)(timestamp_end/(3600*1000));
		
		
		/* ************************************
		 * Measure 8 start
		 * ************************************/
		FileWriter file8=new FileWriter(System.getenv("LAMBDA_HOME") + "/measure/servinglayer.log",true);
		Date date = new Date();
		file8.write(date.toString() + "---------------------------------------\n");
		Long starttime=System.currentTimeMillis();
		int[] edb_count_done  = ElephantDBQuery.countPageviews(elephantdbHost,elephantdbPort,elephantdbDomain_cassandraColumnFamily,pageid,hourStart,hourEnd,cmd);
		int cas_count = CassandraDBQuery.countPageviews(cassandraHost,cassandraPort,dbname,elephantdbDomain_cassandraColumnFamily,pageid,edb_count_done[1]+1,hourEnd,cmd);
		Long endtime = System.currentTimeMillis();
		file8.write(  (hourStart-hourEnd) + (endtime-starttime) + "\n");
		file8.close();
		/* ************************************
		 * Measure 8 end
		 * ************************************/
		
		return edb_count_done[0] + cas_count;
	}
}


class CassandraDBQuery {
	
	public static int countPageviews(String cassandraDomain, int port, String dbname,String columnFamily, String pageid,int hourStart, int hourEnd, String cmd){
	
	  int count = 0;
	  if(hourStart > hourEnd ){
		 return count;
	  }
		
	  Cluster cluster = HFactory.getOrCreateCluster("Test Cluster",cassandraDomain);
	  Keyspace keyspace = HFactory.createKeyspace(dbname, cluster);
	  
	  String keystart= pageid + "/h-" + (hourStart);
	  String keyend= pageid + "/h-" + (hourEnd);
	  
	  RangeSlicesQuery<String, String, String> rangeQuery = HFactory.createRangeSlicesQuery(keyspace,StringSerializer.get() , StringSerializer.get(), StringSerializer.get());
		rangeQuery.setColumnFamily(columnFamily);
		rangeQuery.setRange(null, null, false, Integer.MAX_VALUE);
		rangeQuery.setKeys(keystart, keyend); // start and end range key
		QueryResult<OrderedRows<String,String,String>> res = rangeQuery.execute();
		OrderedRows<String,String,String> rows=res.get();
		
		for(Row row: rows){
	    	 ColumnSlice<String,String> colslice=row.getColumnSlice();
	    	 List<HColumn<String, String>> cols=colslice.getColumns();
	    	 count += cols.size();
	    	 if(cmd.equals("getpageviews")){
	    		 for(HColumn col : cols){
		    		 System.out.print(row.getKey() + " -> ");
		    		 System.out.println(col.getName());
		    	 }
	    	 }
	    }
		
		return count;
	}
	
}

class ElephantDBQuery {
	
    public static int[] countPageviews(String edbDomain, 
    		int port, 
    		String domain,
    		String pageid,
    		int hourStart, 
    		int hourEnd,
    		String cmd) throws DomainNotFoundException, HostsDownException, DomainNotLoadedException, TException, UnsupportedEncodingException{
    	//
    	TTransport transport = new TFramedTransport(new TSocket(edbDomain, port));
    	TBinaryProtocol protocol = new  TBinaryProtocol(transport);
		transport.open();
	
    	ElephantDB.Client client = new ElephantDB.Client(protocol);
    	
    	int count=0;
    	int value=0;
    	int lasthour=0;
    	 String key;
		 Value val;
		 int i=0;
		 int hourDone=0;
		 for(i=hourStart; i<= hourEnd; i++){
			 key=pageid + "/h-" + i;
			 val=client.get(domain, ByteBuffer.wrap(key.getBytes("UTF8")));
			 if(null != val.get_data()){
	    			BigInteger views = new BigInteger(val.get_data());
	    			value =views.intValue();
	    			count +=value;
	    			if(cmd.equals("getpageviews")){
	    				System.out.print(key + " -> ");
	    				System.out.println(value);
	    			}
	    			hourDone=i;
                                
	    	 }
		 }
		
    	int[] countDone=new int[2];
    	countDone[0]=count;
    	countDone[1]=hourDone;
        System.out.println("hourdone:" + countDone[1]);
    	return countDone; 
    	
    	/*
    	int count=0;
    	Set<ByteBuffer> keys = new HashSet<ByteBuffer>();
    	String key=pageid + "/h-";
    	
    	for(int i=hourStart;i < hourEnd; i++){
    		keys.add(ByteBuffer.wrap((key+i).getBytes()));
    	}
    	
		Map<ByteBuffer, Value> results= client.directMultiGet(domain, keys);
		Iterator<Entry<ByteBuffer, Value>> i=results.entrySet().iterator();
		Entry<ByteBuffer,Value> entry;

    	while(i.hasNext()) {
    		//entry=i.next();
    		//lastkey=new String(entry.getKey().array().toString());
    		//System.out.println("key=" + lastkey + " -> ");
    		byte[] byteval =i.next().getValue().get_data();
    		if(null != byteval){
    			BigInteger views = new BigInteger(byteval);
    			count+= views.intValue();
    		}
    	}
    	return count;
    	*/
    }
}
