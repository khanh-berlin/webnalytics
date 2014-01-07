package de.dknguyen.lambda.kafka;

import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import scala.util.Random;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.Pail.TypedRecordOutputStream;

import de.dknguyen.lambda.pail.DataPailStructure;
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

public class ReadKafkaQueueAndWriteToHadoop implements Runnable{

	KafkaStream stream=null;
	int threadNum=0;
	String DIR_NEW_DATA;
	String HDFS;
	FileWriter file4;
	
	public ReadKafkaQueueAndWriteToHadoop(KafkaStream stream, int threadNum, String hdfs,String hadoopFile){
		this.stream=stream;
		this.threadNum=threadNum;
		this.DIR_NEW_DATA=hadoopFile;
		this.HDFS=hdfs;
	}
	
	@Override
	public void run() {
		
		ConsumerIterator<byte[], byte[]> it = this.stream.iterator();
		try {
			file4 = new FileWriter(System.getenv("LAMBDA_HOME") + "/measure/streaming-hadoop.log",true);
			Date date=new Date();
			file4.write(date.toString() + "------------------------------\n");
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	    // read all messages in string
		String messages="";
        while (it.hasNext()){
        	messages = new String(it.next().message());
			try {
				writeToHadoop(messages);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
        }
        try {
			file4.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static Data decodeJsonMessage(JSONObject jsonObject, String messageType) throws ParseException{
		
		Data data=null;
		
		if(messageType.equals("page")){
			data =decodeJsonPage(jsonObject);
			
		}
		else if(messageType.equals("person")){
			data =decodeJsonPerson(jsonObject);
			
		}
		else if(messageType.equals("equiv")){
			data =decodeJsonEquiv(jsonObject);
			
		}
		else if(messageType.equals("pageview")){
			data =decodeJsonPageView(jsonObject);
			
		}
		return data;
	}
	
	public static Data decodeJsonPage(JSONObject jsonObject){
		
		String pedigree=(String)jsonObject.get("pedigree");
		String url=(String)jsonObject.get("url");
		String pageviews=(String)jsonObject.get("pageview");
		
		PagePropertyValue pagePropertyValue= new PagePropertyValue();
    	pagePropertyValue.setPage_views(Integer.parseInt(pageviews));
    	PageID pageID= new PageID();
    	pageID.setUrl(url);
    	PageProperty pageProperty = new PageProperty();
    	pageProperty.setId(pageID);
    	pageProperty.setProperty(pagePropertyValue);
    	
    	return createData(pageProperty,pedigree);
		
	}
	
	public static Data decodeJsonPerson(JSONObject jsonObject){
		
		String pedigree=(String)jsonObject.get("pedigree");
		String personid=(String)jsonObject.get("personid");
		String gender=(String)jsonObject.get("gendertype");
		
		GenderType gendertype=null;
		if(gender.equals("MALE")){
			gendertype=GenderType.MALE;
		}
		else{
			gendertype=GenderType.FEMALE;
		}
		String fullname=(String)jsonObject.get("fullname");
		String city=(String)jsonObject.get("city");
		String state=(String)jsonObject.get("state");
		String country=(String)jsonObject.get("country");
		
		
		PersonID personID=new PersonID();
		if(personid.startsWith("cookie")){
			personID.setCookie(personid);
		}
		else{
			personID.setUser_id(Long.parseLong(personid));
		}
    	
    	PersonPropertyValue personPropertyValue= new PersonPropertyValue();
    	Location location = new Location();
    	location.setCity(city);
    	location.setState(state);
    	location.setCountry(country);
    	personPropertyValue.setGender(gendertype);
    	personPropertyValue.setLocation(location);
    	personPropertyValue.setFull_name(fullname);
    	PersonProperty personProperty = new PersonProperty();
    	personProperty.setProperty(personPropertyValue);
    	personProperty.setId(personID);
    	
    	return createData(personProperty,pedigree);
		
	}
	public static Data decodeJsonEquiv(JSONObject jsonObject){
		
		String pedigree=(String)jsonObject.get("pedigree");
		String personid1=(String)jsonObject.get("personid1");
		String personid2=(String)jsonObject.get("personid2");
		PersonID personID1=new PersonID();
		PersonID personID2=new PersonID();
		if(personid1.startsWith("cookie")){
			personID1.setCookie(personid1);
		}
		else{
			personID1.setUser_id(Long.parseLong(personid1));
		}
		if(personid2.startsWith("cookie")){
			personID2.setCookie(personid2);
		}
		else{
			personID2.setUser_id(Long.parseLong(personid2));
		}
		
		EquivEdge equivEdge=new EquivEdge();
    	equivEdge.setId1(personID1);
    	equivEdge.setId2(personID2);
    	
    	return createData(equivEdge,pedigree);
		
	}
	public static Data decodeJsonPageView(JSONObject jsonObject){
		
		String pedigree=(String)jsonObject.get("pedigree");
		String personid=(String)jsonObject.get("personid");
		String url=(String)jsonObject.get("pageid");
		PersonID personID=new PersonID();
		if(personid.startsWith("cookie")){
			personID.setCookie(personid);
		}
		else{
			personID.setUser_id(Long.parseLong(personid));
		}
    	PageID pageID= new PageID();
    	pageID.setUrl(url);
		
    	Random rand =new Random();
		PageViewEdge pageViewEdge = new PageViewEdge();
    	pageViewEdge.setNonce(rand.nextLong());
    	pageViewEdge.setPerson(personID);
    	pageViewEdge.setPage(pageID);
    	
    	return createData(pageViewEdge,pedigree);
	}
	
	public static Data createData(Object obj,String timestamp_in_second){
		
		
		DataUnit dataunit=new DataUnit();
       	
		
    	if(obj instanceof PageProperty){	
			dataunit.setPage_property((PageProperty)obj);
		}
		if(obj instanceof PersonProperty){
			dataunit.setPerson_property((PersonProperty)obj);
		}
		if(obj instanceof EquivEdge){
			dataunit.setEquiv((EquivEdge)obj);
		}
		if(obj instanceof PageViewEdge){
			dataunit.setPage_view((PageViewEdge)obj);
		}
		
		Pedigree pedigree=new Pedigree();
    	pedigree.setTrue_as_of_secs(Integer.parseInt(timestamp_in_second));
    	Data data=new Data();
    	data.setDataunit(dataunit);
    	data.setPedigree(pedigree);
    	
    	return data;
    	
	}
	
	public void writeToHadoop(String jsonMessages) throws ParseException, IOException, URISyntaxException{
		
		JSONParser parser = new JSONParser();
		Object obj =parser.parse(jsonMessages);
		JSONObject jsonObject =  (JSONObject) obj;
		String messageType=(String) jsonObject.get("messagetype");
		Pail hadoopPail = new Pail(HDFS+DIR_NEW_DATA);
		TypedRecordOutputStream os = hadoopPail.openWrite();
		
		if(messageType.equals("messages")){
			JSONArray messages = (JSONArray) jsonObject.get("messages");
			List<Data> datas=decodeJsonMessages(messages);
			
			/* ******************************************
			 * Measure 5 start
			 * *****************************************/
			long starttime=System.currentTimeMillis();
			for(Data data:datas){
				os.writeObject(data);
			}
			long endtime=System.currentTimeMillis();
			file4.write(datas.size()+ ";" + (endtime - starttime) + "\n");
			file4.flush();
			/* *****************************************
			 * Measure 5 end
			 * *****************************************/
		}
		else{
			Data data=decodeJsonMessage(jsonObject,messageType);
			os.writeObject(data);
		}
		os.close();
	}

	private List<Data> decodeJsonMessages(JSONArray messages) throws ParseException {
		List<Data> datas = new ArrayList<Data>();
		JSONObject message;
		String messageType;
		Data data;
		int size=messages.size();
		for(int i = 0 ; i < size ; i++){
		    message=(JSONObject) messages.get(i);
		    messageType=(String) message.get("messagetype");
		    data = decodeJsonMessage(message,messageType);
		    datas.add(data);
		}
		return datas;
	}
}

