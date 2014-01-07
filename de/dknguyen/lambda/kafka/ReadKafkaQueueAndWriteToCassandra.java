package de.dknguyen.lambda.kafka;

import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import de.dknguyen.lambda.cassandra.CassandraUtils;
import de.dknguyen.lambda.cassandra.ColGroup;
import de.dknguyen.lambda.cassandra.Equiv;
import de.dknguyen.lambda.cassandra.Page;
import de.dknguyen.lambda.cassandra.Pageview;
import de.dknguyen.lambda.cassandra.Person;

public class ReadKafkaQueueAndWriteToCassandra implements Runnable{

	KafkaStream stream=null;
	int threadNum=0;
	String cassandraHost;
	int cassandraPort;
	TTransport transport;
	TProtocol protocol;
	FileWriter file2;
	
	public ReadKafkaQueueAndWriteToCassandra(KafkaStream stream, int threadNum, String cassandra) throws TTransportException, IOException{
		this.stream=stream;
		this.threadNum=threadNum;
		String[] cassandraURI=cassandra.split(":");
		this.cassandraHost=cassandraURI[0];
		this.cassandraPort=Integer.valueOf(cassandraURI[1]);
		this.transport=CassandraUtils.openConnection(cassandraHost,cassandraPort);
	}
	
	@Override
	public void run() {
		ConsumerIterator<byte[], byte[]> it = this.stream.iterator();
		try {
			file2= new FileWriter(System.getenv("LAMBDA_HOME") + "/measure/speedlayer.log",true);
	    	Date date= new Date();
	    	file2.write(date.toString() + "------------------------------\n");
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		String messages="";
	    
		while (it.hasNext()){
        	messages = new String(it.next().message());
        	
			try {
				writeToCassandra(messages);
			} catch (InvalidRequestException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
		
		this.transport.close();
		try {
			this.file2.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void writeToCassandra(String inputJsonString) throws ParseException, InvalidRequestException, TException, IOException{
		
		JSONParser parser = new JSONParser();
		Object obj =parser.parse(inputJsonString);
		JSONObject jsonObject =  (JSONObject) obj;
		String messageType=(String) jsonObject.get("messagetype");
				
		if(messageType.equals("messages")){
			JSONArray messages = (JSONArray) jsonObject.get("messages");
			
			/* ******************************
			 * measuring 2 start
			 *******************************/
			long starttime=System.currentTimeMillis();
			List<ColGroup> rows=decodeJsonMessages(messages);
		    CassandraUtils.insertColGroups2(this.transport,rows);
		    long endtime=System.currentTimeMillis();
		    this.file2.write(rows.size() + ";" + (endtime-starttime) + "\n" );
		    this.file2.flush();
		    /* *****************************
		     * measuring 2 end
		    ********************************/
		}
		
		else{
			CassandraUtils.insertColGroup(this.transport,decodeJsonMessage(jsonObject,messageType));
		}
		
	}
	
	public static ColGroup decodeJsonMessage(JSONObject message, String messageType) throws UnsupportedEncodingException, InvalidRequestException, UnavailableException, TimedOutException, TException, MalformedURLException{
		
		ColGroup data=null;
		
		if(messageType.equals("page")){
			data=decodeJsonPage(message);
		}
		else if(messageType.equals("person")){
			data=decodeJsonPerson(message);
		}
		else if(messageType.equals("equiv")){
			data=decodeJsonEquiv(message);
		}
		else if(messageType.equals("pageview")){
			data=decodeJsonPageview(message);
		}
		return data;
	}
	
	public static List<ColGroup> decodeJsonMessages(JSONArray messages) throws UnsupportedEncodingException, InvalidRequestException, UnavailableException, TimedOutException, TException, MalformedURLException{
		
		List<ColGroup> colGroups = new ArrayList<ColGroup>();
		JSONObject message;
		String messageType;
		ColGroup colGroup;
		int size=messages.size();
		for(int i = 0 ; i < size ; i++){
		    message=(JSONObject) messages.get(i);
		    messageType=(String) message.get("messagetype");
		    colGroup = decodeJsonMessage(message,messageType);
		    colGroups.add(colGroup);
		}
		
		return colGroups;
	}

	public static ColGroup decodeJsonPage(JSONObject jsonObject) throws InvalidRequestException, UnavailableException, TimedOutException, TException, UnsupportedEncodingException, MalformedURLException{
		
		String pedigree =(String)jsonObject.get("pedigree");
		String url=normalizeURL((String)jsonObject.get("url"));
		String pageview=(String)jsonObject.get("pageview");
		
		return Page.createColGroup(pedigree, url, pageview);
		
	}
	
	public static ColGroup decodeJsonPerson(JSONObject jsonObject) throws InvalidRequestException, TException, UnsupportedEncodingException{
		
		String pedigree=(String)jsonObject.get("pedigree");
		String personid=(String)jsonObject.get("personid");
		String gendertype=(String)jsonObject.get("gendertype");
		String fullname=(String)jsonObject.get("fullname");
		String city=(String)jsonObject.get("city");
		String state=(String)jsonObject.get("state");
		String country=(String)jsonObject.get("country");
		
		return Person.createColGroup(pedigree, personid, gendertype, fullname, city, state, country);
		
	}
	
	public static ColGroup decodeJsonEquiv(JSONObject jsonObject) throws InvalidRequestException, TException, UnsupportedEncodingException{
		
		String pedigree =(String)jsonObject.get("pedigree");
		String personid1=(String)jsonObject.get("personid1");
		String personid2=(String)jsonObject.get("personid2");
		return Equiv.createColGroup(pedigree, personid1, personid2);
		
	}
	
	public static ColGroup decodeJsonPageview(JSONObject jsonObject) throws UnsupportedEncodingException, InvalidRequestException, UnavailableException, TimedOutException, TException, MalformedURLException{
		String pedigree =(String)jsonObject.get("pedigree");
		String pageid=normalizeURL((String)jsonObject.get("pageid"));
		String personid=(String)jsonObject.get("personid");
		
		return Pageview.createColGroup(pedigree, pageid, personid);
	}
	
	public static String normalizeURL(String urlStr) throws MalformedURLException{
		URL url = new URL(urlStr);
		return url.getProtocol() + "://" + url.getHost() + url.getPath();	
	}
	
	
}

