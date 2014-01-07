package de.dknguyen.lambda;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.Pail.TypedRecordOutputStream;

import de.dknguyen.lambda.cassandra.ColGroup;
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
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import scala.util.Random;
import de.dknguyen.lambda.kafka.ReadKafkaQueueAndWriteToHadoop;

public class StreamingQueueToHadoop {
	
	
	public String HDFS = "hdfs://b-sso-02d:54310";
	public Map<String,String> HADOOP_DIR_STRUCTURE;
	public String zookeeper;
	public String topic;
	public int threads;
	
	/*
	 * Read message from zookeeper
	 * */
	
	public StreamingQueueToHadoop(String zookeeper,String topic, int threads, String HDFS, Map<String,String> HADOOP_DIR_STRUCTURE){
		this.HDFS=HDFS;
		this.HADOOP_DIR_STRUCTURE = HADOOP_DIR_STRUCTURE;
		this.zookeeper=zookeeper;
		this.topic=topic;
		this.threads=threads;
	}
	
	public void startStreaming(){

		/** 
		 * Create consumer : zu zookeeper
		 */
		ConsumerConnector consumer;
		Properties props = new Properties();
        //props.put("zookeeper.connect", "b-web-05:2181");
        props.put("zookeeper.connect", this.zookeeper);
        props.put("group.id", "batchlayer_consumer_group");
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
		
        /**
         * Create multiple streams zu topic: aus consumer 
         */
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        // config streams zu topic
        topicCountMap.put(topic, new Integer(this.threads));
        // create streams
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        // get topic aus streams 
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(this.topic);
        
        /**
         * Working with messages aus multiple streams
         * */
        int thread=0;
        for (final KafkaStream stream : streams) {
        	new Thread(new ReadKafkaQueueAndWriteToHadoop(stream,thread,HDFS,HADOOP_DIR_STRUCTURE.get("DIR_NEW_DATA"))).start();   	
 	        thread++;
        }
        
	}
	   
}
