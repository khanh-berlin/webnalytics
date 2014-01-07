package de.dknguyen.lambda;

import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.json.simple.JSONObject;

import de.dknguyen.lambda.thrift.Data;
import de.dknguyen.lambda.thrift.DataUnit;
import de.dknguyen.lambda.thrift.EquivEdge;
import de.dknguyen.lambda.thrift.GenderType;
import de.dknguyen.lambda.thrift.Location;
import de.dknguyen.lambda.thrift.PageID;
import de.dknguyen.lambda.thrift.PageProperty;
import de.dknguyen.lambda.thrift.PagePropertyValue;
import de.dknguyen.lambda.thrift.PageViewEdge;
import de.dknguyen.lambda.thrift.Pedigree;
import de.dknguyen.lambda.thrift.PersonID;
import de.dknguyen.lambda.thrift.PersonProperty;
import de.dknguyen.lambda.thrift.PersonPropertyValue;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import backtype.hadoop.ThriftSerialization;

public class StreamingNewDataToQueue {
		
	 public String kafkaserver;
	 public String topic;
	 public FileWriter file1;
	 
	 public StreamingNewDataToQueue(String kafkaserver,String topic) throws IOException{
		 this.kafkaserver=kafkaserver;
		 this.topic=topic;
		 file1=new FileWriter(System.getenv("LAMBDA_HOME") + "/measure/streaming-queue.log",true);
		 Date date=new Date();
		 file1.write(date.toString() + "------------------------------\n");
	 }
	
	 public void generateAndStreamingDataToQueue(String dateStart,String dateEnd,int batch, String factType) throws ParseException, IOException{
         Properties props = new Properties();
         props.put("metadata.broker.list", this.kafkaserver);
         props.put("serializer.class", "kafka.serializer.StringEncoder");
         //props.put("serializer.class", "backtype.hadoop.ThriftSerialization");
         //props.put("partitioner.class", "de.dknguyen.lambda.kafka.KafkaExamplesProducerPartitioner");
         props.put("request.required.acks", "1");

         ProducerConfig config = new ProducerConfig(props);
         Producer<String, String> producer = new Producer<String, String>(config);

         SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy|hh:mm:ss");
 		 Date date_start = sdf.parse(dateStart);
 		 Date date_end = sdf.parse(dateEnd);
 		 long timestamp_start=date_start.getTime()/1000;
 		 long timestamp_end=date_end.getTime()/1000;
 		 long measure_facts=timestamp_end-timestamp_start;
 		 long measure_time = 0;
         long timestamp_rest;
         long timestamp_next;
         
         
         // init pages, persons, equivs
         List<String> facts = new ArrayList<String>();
         if(null !=factType && factType.equals("allfact")){
        	 facts=genJSONFacts(timestamp_start,0,"allfact");
        	 for (String fact:facts) {
        		 KeyedMessage<String, String> data = new KeyedMessage<String, String>(this.topic, fact);
        		 producer.send(data);
        	 }
         }
         
         // pageviews
         
 		 while(timestamp_start < timestamp_end){
 			timestamp_rest = timestamp_end - timestamp_start;
 			 
 			 if(timestamp_rest > batch){
 				 timestamp_next=timestamp_start+batch;
 			 }else{
 				timestamp_next=timestamp_start+timestamp_rest;
 			 }
 			 
 			 /* ************************************
 			  * measuring 1 start                        
 			  **************************************/
 			facts=genJSONFacts(timestamp_start,timestamp_next,"pageviews");
 			long measure_starttime=System.currentTimeMillis();
 			 for (String fact:facts) {
 				 KeyedMessage<String, String> keyedMessages = new KeyedMessage<String, String>(this.topic, fact);
 				 producer.send(keyedMessages);
 			 }
 			 long measure_endtime=System.currentTimeMillis();
 			 measure_time += (measure_endtime -measure_starttime);
 			 /* ************************************
 			  * measuring 1 end 
 			  * ************************************/
 			 timestamp_start=timestamp_next+1;
 		 }
 		 producer.close();
 		 this.file1.write(measure_facts + ";" + measure_time);
		 this.file1.flush();
 		 this.file1.close();
 		 
	 }

	public static List<String> genJSONFacts(long timestamp_second_start,long timestamp_second_end,String factType){
		
		List<String> messages =new ArrayList<String>();
		int person_pair=4;
		int page_pair=4;
		
		if(null == factType || factType.equals("allfact")){
		   messages.addAll(genJSONPersons(timestamp_second_start,person_pair));
		   messages.addAll(genJSONEquivs(timestamp_second_start,person_pair));
		   messages.addAll(genJSONPages(timestamp_second_start,page_pair));
		   //facts.addAll(genJSONPageviews(timestamp_second_start,timestamp_second_end, 1,person_pair,page_pair));
		}
		else if(factType.equals("pageviews")){
			List<String> pageviews=genJSONPageviews(timestamp_second_start,timestamp_second_end, person_pair,page_pair);
			messages.addAll(groupJSONMessages(pageviews));
			
		}
		
		return messages;
	}
	
	 static List<String> groupJSONMessages(List<String> jsonFacts){
		List<String> messages= new ArrayList<String>();
		String jsonFactsString=null;
		jsonFactsString = "{\"messagetype\": \"messages\",\"messages\":[";
		for(String jsonFact:jsonFacts){
			jsonFactsString += jsonFact + ",";
		}
		jsonFactsString += "]}";
		messages.add(jsonFactsString);
		return messages;
	}
	
	/*
	 * pedigree 111 -> http://blog.domain_i.org
	 * pedigree 111 -> http://blog.domain_i.org?article=1
	 * */
	public static List<String> genJSONPages(long start_timestamp_in_second, int page_pair_amount){
	   
		List<String> pages = new ArrayList<String>();
		int pageview=0;
		JSONObject obj=new JSONObject();
		
		long timestamp_in_second=start_timestamp_in_second;
		
		for(int i=0;i<page_pair_amount;i++){
		  pageview=1+ i%2;	
		  
		  obj.put("messagetype","page");
		  obj.put("pedigree", Long.toString(timestamp_in_second));
		  obj.put("url", "http://blog.d" + i + ".org");
		  obj.put("pageview", Integer.toString(pageview));
		  pages.add(obj.toJSONString());
		  obj.clear();
		  
		  obj.put("messagetype","page");
		  obj.put("pedigree", Long.toString(timestamp_in_second));
		  obj.put("url", "http://blog.d" + i + ".org?article=1");
		  obj.put("pageview", Integer.toString(pageview));
		  pages.add(obj.toJSONString());
		  obj.clear();
		  
		  timestamp_in_second++;
	   }
		
	   return pages;
	}
	
	/*
	 * pedigree 123 -> user=  i
	 * pedigree 123 -> user= cookie_i
	 * */
	public static List<String> genJSONPersons(long start_timestamp_in_second, int person_pair_amount){
		List<String> persons=new ArrayList<String>();
		//StringBuffer 
		String gender[] = {"MALE","FEMALE"};
		Random rand = new Random();
		long timestamp_in_second=start_timestamp_in_second;
		
		JSONObject obj = new JSONObject();
		
		for(int i=1;i<=(person_pair_amount);i++){
			
			obj.put("messagetype","person");
			obj.put("pedigree",Long.toString(timestamp_in_second));
			obj.put("personid","cookie_" + i);
			obj.put("gendertype",gender[rand.nextInt(2)]);
			obj.put("fullname","Dinh Khanh Nguyen");
			obj.put("city","Berlin");
			obj.put("state","Berlin");
			obj.put("country","Germany");
			persons.add(obj.toJSONString());
			obj.clear();
			
			obj.put("messagetype","person");
			obj.put("pedigree",Long.toString(timestamp_in_second));
			obj.put("personid","" + i);
			obj.put("gendertype",gender[rand.nextInt(2)]);
			obj.put("fullname","Dinh Khanh Nguyen");
			obj.put("city","Berlin");
			obj.put("state","Berlin");
			obj.put("country","Germany");
			persons.add(obj.toJSONString());
			obj.clear();
			timestamp_in_second++;
		}
		return persons;
	}
	
	/*
	 * pedigree 111 -> cookie_i <> i
	 * */
	
	public static List<String> genJSONEquivs(long start_timestamp_in_second, long persons){
		
		List<String> equivs= new ArrayList<String>();
		JSONObject obj= new JSONObject();
		long timestamp_in_second=start_timestamp_in_second;
		
		for(int i=1;i<=(persons);i++){
			
			obj.put("messagetype","equiv");
			obj.put("pedigree",Long.toString(timestamp_in_second++));
			obj.put("personid1", Integer.toString(i));
			obj.put("personid2", "cookie_" + i);
			equivs.add(obj.toJSONString());
			obj.clear();
		}
		
		return equivs;
	}
	
	/*
	 * pageview amount = (timestamp_second_end-timestamp_second_start) * pageview_pro_second
	 * pedigree 111 -> pageid=random(1 to page_pair_amount) : personid=random(1 to person_pair_amount)
	 * */
	public static List<String> genJSONPageviews(
					long timestamp_second_start, 
					long timestamp_second_end, 
					int person_pair_amount,
					int page_pair_amount){
		
		List<String> views = new ArrayList<String>();
		String personIDType[] = {"cookie_",""};
		String path[]={".org",".org?article=1"};
		Random rand = new Random();
		JSONObject obj= new JSONObject();
		
		for(long i=timestamp_second_start;i<=timestamp_second_end;i++){
			
				obj.put("messagetype","pageview");
				obj.put("pedigree", Long.toString(i));
				obj.put("personid",personIDType[rand.nextInt(2)] + (rand.nextInt(person_pair_amount)+1));
				obj.put("pageid","http://blog.d" + (rand.nextInt(page_pair_amount)+1) + path[rand.nextInt(2)]);
				views.add(obj.toJSONString());
				obj.clear();
			
		}
		return views;
	}
}
