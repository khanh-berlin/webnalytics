package de.dknguyen.lambda.cassandra;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;


public class CassandraUtils {
	
	private static final String keyspace ="webanalytics";
	public String cassandraDomain;
	public int cassandraPort;
	
	public CassandraUtils(String cassandraDomain,int cassandraPort){
		this.cassandraDomain=cassandraDomain;
		this.cassandraPort=cassandraPort;
	}
	
	
	public static TTransport openConnection(String cassandraDomain,int cassandraPort) throws TTransportException{
		 TTransport transport = new TFramedTransport(new TSocket(cassandraDomain, cassandraPort));
	     transport.open();
	     return transport;
	}
	 
	public static void insertColGroups(TTransport transport,List<ColGroup> colGroups) throws InvalidRequestException, UnavailableException, TimedOutException, TException {
		
	     TProtocol protocol = new TBinaryProtocol(transport);
	     Cassandra.Client client = new Cassandra.Client(protocol);
	     
	     client.set_keyspace(keyspace);
	     
	     // wrapper
	     Map<ByteBuffer, Map<String,List<Mutation>>> rowDefinitionList = new HashMap<ByteBuffer, Map<String,List<Mutation>>>();
		 Map<String,List<Mutation>> columnFamilyValues;
		 List<Mutation> insertion_list;
	     Mutation columns_mutation;
	     
	     // colGroup (=one row in SQL) 
		 List<Column> columns; 
		 
		 
		 for(ColGroup colGroup: colGroups){
			 
			 // get value
			 columns = colGroup.getColGroup();
			 String colFamily =colGroup.getColFamily();
			 ByteBuffer key = colGroup.getKey();
			 
			 // create wrapper for colGroup
			 columnFamilyValues = new HashMap<String,List<Mutation>>(); // colFamily + key + colGroup(one row)
			 insertion_list = new ArrayList<Mutation>(); // = colGroup (one rows) 
			 columns_mutation = new Mutation(); // = colGroup (one rows)
			 
			 // insert value to wrapper
			 for(Column column: columns){
				 columns_mutation.setColumn_or_supercolumn(new ColumnOrSuperColumn().setColumn(column));
			 }
			 insertion_list.add(columns_mutation);
			 columnFamilyValues.put(colFamily,insertion_list);
			 
			 rowDefinitionList.put(key, columnFamilyValues); // rowDefinitionList = multi row
		 }
		 client.batch_mutate(rowDefinitionList, ConsistencyLevel.ONE);
	}

	public static void insertColGroups2(TTransport transport,List<ColGroup> colGroups) throws InvalidRequestException, TException{
		TProtocol protocol = new TBinaryProtocol(transport);
	    Cassandra.Client client = new Cassandra.Client(protocol);
		client.set_keyspace(CassandraUtils.keyspace);
		ColumnParent parent = new ColumnParent();
		
		List<Column> columns;
		String colFamily;
		ByteBuffer key;
		for(ColGroup colGroup : colGroups){
			columns = colGroup.getColGroup();
			colFamily=colGroup.getColFamily();
			key=colGroup.getKey();
			parent.setColumn_family(colFamily);
			for(Column column: columns){
				client.insert(key, parent, column, ConsistencyLevel.ONE);
			}
		}
	}
	
	public static void insertColGroup(TTransport transport, ColGroup colGroup) throws InvalidRequestException, TException {
		
		TProtocol protocol = new TBinaryProtocol(transport);
	    Cassandra.Client client = new Cassandra.Client(protocol);
		client.set_keyspace(CassandraUtils.keyspace);
		ColumnParent parent = new ColumnParent();
		
		List<Column> columns = colGroup.getColGroup();
		String colFamily=colGroup.getColFamily();
		ByteBuffer key=colGroup.getKey();
		
		parent.setColumn_family(colFamily);
		
		for(Column column: columns){
			client.insert(key, parent, column, ConsistencyLevel.ONE);
		}
	}
	
	public static String normalizeURL(String urlStr) throws MalformedURLException{
		URL url = new URL(urlStr);
		return url.getProtocol() + "://" + url.getHost() + url.getPath();	
	}
	
	public static void main(String[] args) throws UnsupportedEncodingException, InvalidRequestException, TException{
		
		String[] domainURI=args[0].split(":");
		String domain=domainURI[0];
		int port=Integer.valueOf(domainURI[1]);
		System.out.println(domain);
		System.out.println(port);
		
		TTransport transport = new TFramedTransport(new TSocket("localhost", 9160 ));
		TProtocol protocol = new TBinaryProtocol(transport);
		
	    Cassandra.Client client = new Cassandra.Client(protocol);
	    transport.open();
		client.set_keyspace("webanalytics");

		
		String pedigree="12";
		String pageid ="http://blog.domain_10.or";
		String personid ="123";
		ColGroup colGroup=Pageview.createColGroup(pedigree, pageid, personid);
	
		ColumnParent parent = new ColumnParent();
		List<Column> columns = colGroup.getColGroup();
		String colFamily=colGroup.getColFamily();
		System.out.println(colFamily);
		ByteBuffer key=colGroup.getKey();
		parent.setColumn_family(colFamily);
		
		for(Column column: columns){
			System.out.println(column.toString());
			client.insert(key, parent, column, ConsistencyLevel.ONE);
		}
		
		transport.close();
	}
	
}
