package de.dknguyen.lambda;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jcascalog.Api;
import jcascalog.Subquery;
import jcascalog.op.Count;
import jcascalog.op.Sum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import cascading.tap.Tap;

import com.backtype.cascading.tap.PailTap;
import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.PailSpec;
import com.google.common.collect.Maps;
import com.twitter.maple.tap.StdoutTap;

import de.dknguyen.lambda.cascalog.EmitGranularities;
import de.dknguyen.lambda.cascalog.ExtractPageViewFields;
import de.dknguyen.lambda.cascalog.ToHourBucket;
import de.dknguyen.lambda.cascalog.ToSerializedLong;
import de.dknguyen.lambda.cascalog.ToUrlBucketedKey;
import de.dknguyen.lambda.pail.DataPailStructure;
import de.dknguyen.lambda.pail.SplitDataPailStructure;
import de.dknguyen.lambda.thrift.DataUnit;
import elephantdb.generated.DomainNotFoundException;
import elephantdb.generated.DomainNotLoadedException;
import elephantdb.generated.HostsDownException;

public class Main {
	
    
    public static Map<String,String> HADOOP_DIR_STRUCTURE = new HashMap<String,String>();
    static{
    	HADOOP_DIR_STRUCTURE.put("DIR_NEW_DATA", "/swa/newData");
    	HADOOP_DIR_STRUCTURE.put("DIR_MASTER_DATA", "/swa/masterData");
    	HADOOP_DIR_STRUCTURE.put("DIR_EDB_PAGEVIEWS", "/outputs/edb/pageviews");
    	
    	HADOOP_DIR_STRUCTURE.put("DIR_SNAPSHOT", "/tmp/newDataSnapshot");
    	HADOOP_DIR_STRUCTURE.put("DIR_SHREDDED_DATA", "/tmp/shredded");
    	HADOOP_DIR_STRUCTURE.put("DIR_NORMALIZED_URLS", "/tmp/normalized_urls");
    	HADOOP_DIR_STRUCTURE.put("DIR_TEMP", "/tmp");
    }
	
	public static void main(String[] args) throws IOException, URISyntaxException, DomainNotFoundException, HostsDownException, DomainNotLoadedException, TException, ParseException{
		
		boolean isArguments=true;
		//configPail();
		
		if(args[0].equals("--batchlayer") && args[1].equals("start") && args[2].equals("--hdfs") && !args[3].isEmpty()){
			String HDFS=args[3];
			
			if(!HDFS.isEmpty()){
				setupHadoopDirectory(HDFS);
				
				BatchLayerStorage batchStorage=new BatchLayerStorage(HDFS,HADOOP_DIR_STRUCTURE);
				batchStorage.insertNewDataIntoMaster();
				
				BatchLayerPrecomputing.precomputingMasterdataAndStoreInEDB(HDFS,HADOOP_DIR_STRUCTURE);
			}
			else{
				isArguments=false;
			}
		}
		
		
		else if(args[0].equals("--speedlayer") && args[1].equals("start")){
			
			String zookeeper="";
			String fromTopic="";
			String toCassandra="";
			int threads=1;
			
			if(args[2].equals("--zookeeper") && !args[3].isEmpty() 
					&& args[4].equals("--topic") && !args[5].isEmpty() 
					&& args[6].equals("--cassandra") && !args[7].isEmpty()){
				zookeeper=args[3];
				fromTopic=args[5];
				toCassandra=args[7];
			}
			if(args[8].equals("--threads") && !args[9].isEmpty()){
				threads=Integer.valueOf(args[9]);
			}
			
			if(!zookeeper.isEmpty() && !fromTopic.isEmpty() && !toCassandra.isEmpty()){
				SpeedLayer speedLayer = new SpeedLayer(zookeeper,fromTopic, threads, toCassandra);
				speedLayer.startStreamingPrecomputingStorage();
			}
			else{
				isArguments=false;
			}
		}
		
		
		else if(args[0].equals("--streaming") && args[1].equals("queue") && ! args[2].isEmpty() && ! args[3].isEmpty()){
			
			String kafkaserver="";
			String toTopic="";
			String dateStart=args[6];
			String dateEnd=args[7];
			int batch=Integer.valueOf(args[8]);
			String factType=args[9];
			
			if(args[2].equals("--kafka") && !args[3].isEmpty() && args[4].equals("--topic") && !args[5].isEmpty()){
				kafkaserver=args[3];
				toTopic=args[5];
			}
			
			if(!kafkaserver.isEmpty() && !toTopic.isEmpty()){
				StreamingNewDataToQueue streaming=new StreamingNewDataToQueue(kafkaserver,toTopic); 
				streaming.generateAndStreamingDataToQueue(dateStart,dateEnd,batch,factType);
			}
			else{
				isArguments=false;
			}
		}
		
		
		else if(args[0].equals("--streaming") && args[1].equals("hadoop")){
			String zookeeper="";
			String fromTopic="";
			String HDFS="";
			int threads=1;
			
			if(args[2].equals("--zookeeper") && !args[3].isEmpty() && args[4].equals("--topic") && !args[5].isEmpty()){
				zookeeper=args[3];
				fromTopic=args[5];
			}
			if(args[6].equals("--hdfs") && !args[7].isEmpty()){
				HDFS=args[7];
			}
			if(args[8].equals("--threads") && !args[9].isEmpty()){
				threads=Integer.valueOf(args[9]);
			}
			if(!zookeeper.isEmpty() && !fromTopic.isEmpty() && !HDFS.isEmpty()){
				setupHadoopDirectory(HDFS);
				StreamingQueueToHadoop streaming=new StreamingQueueToHadoop(zookeeper,fromTopic,threads,HDFS,HADOOP_DIR_STRUCTURE);
				streaming.startStreaming();
			}
			else{
				isArguments=false;
			}
		}
		
		else if(args[0].equals("--serving") && ! args[1].isEmpty()){
			
			String cmd=args[1];
			
			String cassandraHost = null;
			int cassandraPort = 0;
			String elephantdbHost = null;
			int elephantdbPort = 0;
			String pageid = null;
			String dateStart = null;
			String dateEnd = null;
			
			if(args[2].equals("--cassandra") && !args[3].isEmpty()){
				String[] cassandraURI = args[3].split(":");
				cassandraHost=cassandraURI[0];
				cassandraPort=Integer.valueOf(cassandraURI[1]);
				
			}
			if(args[4].equals("--elephantdb") && !args[5].isEmpty()){
				String[] elephantURI = args[5].split(":");
				elephantdbHost = elephantURI[0];
				elephantdbPort = Integer.valueOf(elephantURI[1]);
			}
			if(args[6].equals("--pageid") && !args[7].isEmpty()){
				pageid=args[7];
			}
			if(args[8].equals("--datestart") && !args[9].isEmpty()){
				dateStart=args[9];
			}
			if(args[10].equals("--dateend") && !args[11].isEmpty()){
				dateEnd=args[11];
			}
			
			if(!cassandraHost.isEmpty() &&  cassandraPort!=0 &&  !elephantdbHost.isEmpty() && 
					elephantdbPort !=0 && !pageid.isEmpty()  && !dateStart.isEmpty() && !dateEnd.isEmpty()){
			
				if(cmd.equals("countpageviews") || cmd.equals("getpageviews")){
					
					int count=ServingLayer.countPageviews(
						cassandraHost, 
						cassandraPort, 
						elephantdbHost, 
						elephantdbPort, 
						"webanalytics", 
						"pageviews", 
						pageid, 
						dateStart, 
						dateEnd,cmd);
					System.out.println("Pageviews=" + count);
				}
				else{
					isArguments=false;
				}
			}
			
		}
		
		else if(args[0].equals("--test-batchviews") && ! args[1].isEmpty()){
			String hdfs=args[1];
			queryMasterDataToPageviews(hdfs);
		}
		
		else if(args[0].equals("--test-elephantdb") && ! args[1].isEmpty()){
			String cassandra=args[1];
			queryMasterDataToPageviews(args[1]);
		}
		
		else {
			isArguments=false;
		}
		
		if(!isArguments){
			System.out.println("Wrong arguments. Please try again with correct arguments.");
		}
	}
	
	 private static void setupHadoopDirectory(String HDFS) throws IOException, URISyntaxException {
		 DistributedFileSystem dfs = new DistributedFileSystem();
	     dfs.initialize(new URI(HDFS), new Configuration());
	     Path pathNewData = new Path(HADOOP_DIR_STRUCTURE.get("DIR_NEW_DATA"));
	     Path pathMasterData = new Path(HADOOP_DIR_STRUCTURE.get("DIR_MASTER_DATA"));
	     Path pathTempDir = new Path(HADOOP_DIR_STRUCTURE.get("DIR_TEMP"));
		 
		 if (!dfs.exists(pathNewData)){
	    	Pail.create(HDFS+HADOOP_DIR_STRUCTURE.get("DIR_NEW_DATA"),new DataPailStructure());
	     }
	     if (!dfs.exists(pathMasterData)){
	        Pail.create(HDFS+HADOOP_DIR_STRUCTURE.get("DIR_MASTER_DATA"),new SplitDataPailStructure());
	     }
	     if (dfs.exists(pathTempDir)){
		     dfs.delete(pathTempDir, true);
		 }
	}	
	 
	 private static void configPail() throws IOException {
	        Map<String, String> conf = Maps.newHashMap();
	        String sers = "backtype.hadoop.ThriftSerialization";
	        sers += ",";
	        sers += "org.apache.hadoop.io.serializer.WritableSerialization";
	        conf.put("io.serializations", sers);
	        //conf.put("fs.default.name", "hdfs://b-sso-02d:54310");
	        Api.setApplicationConf(conf);
	 }
	 
	 
	 /***
	  * 
	  * Test function
	  */
	 public static void queryMasterDataToPageviews(String hdfs){
	    	
	   	   //Tap source= attributeTap(HDFS+HADOOP_DIR_STRUCTURE.get("DIR_MASTER_DATA"), DataUnit._Fields.PAGE_VIEW);
	       Tap source= attributeTap(hdfs+HADOOP_DIR_STRUCTURE.get("DIR_NORMALIZED_URLS"), DataUnit._Fields.PAGE_VIEW);
	        Subquery hourbucket = new Subquery("?url", "?hour-bucket", "?count")
	        		.predicate(source, "_","?pageview")
	                .predicate(new ExtractPageViewFields(), "?pageview").out("?url", "_", "?timestamp")
	                .predicate(new ToHourBucket(), "?timestamp").out("?hour-bucket")
	                .predicate(new Count(), "?count");
	   	
	        Subquery pageViews = new Subquery("?url", "?granularity", "?bucket", "?total-pageviews")
	        .predicate(hourbucket, "?url", "?hour-bucket", "?count")
	        .predicate(new EmitGranularities(), "?hour-bucket").out("?granularity", "?bucket")
	        .predicate(new Sum(), "?count").out("?total-pageviews");
	        
	        Subquery edbKeyValue =
	  				new Subquery("?key", "?value")
	  				.predicate(pageViews,
	  				"?url", "?granularity", "?bucket", "?total-pageviews")
	  				.predicate(new ToUrlBucketedKey(),
	  				"?url", "?granularity", "?bucket")
	  				.out("?key")
	  				.predicate(new ToSerializedLong(), "?total-pageviews")
	  				.out("?value");
	        
	        //
	        Api.execute(new StdoutTap(),edbKeyValue);
	    }
	 
	 private static PailTap attributeTap(
	            String path,
	            final DataUnit._Fields... fields) {
	        PailTap.PailTapOptions opts = new PailTap.PailTapOptions();
	        opts.attrs = new List[] {
	                new ArrayList<String>() {{
	                    for(DataUnit._Fields field: fields) {
	                        add("" + field.getThriftFieldId());
	                    }
	                }}
	        };
	        opts.spec = new PailSpec(new SplitDataPailStructure());
	        return new PailTap(path, opts);
	    }

}
