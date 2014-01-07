package de.dknguyen.lambda;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Iface;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
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
import de.dknguyen.lambda.kafka.ReadKafkaQueueAndWriteToCassandra;


public class SpeedLayer {
	
	/*
	 * Read message from zookeeper
	 * */
	public String zookeeper;
	public String topic;
	public int threads;
	public String toCassandra;
	
	public SpeedLayer(String zookeeper,String topic, int threads,String toCassandra){
		this.zookeeper=zookeeper;
		this.topic=topic;
		this.threads=threads;
		this.toCassandra=toCassandra;
	}
	
	public void startStreamingPrecomputingStorage() throws TTransportException, IOException{

		/** 
		 * Create consumer : zu zookeeper
		 */
		ConsumerConnector consumer;
		Properties props = new Properties();
        //props.put("zookeeper.connect", "b-web-05:2181");
		props.put("zookeeper.connect",this.zookeeper);
        props.put("group.id", "speedlayer_consumser_group");
        /*
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        */
        props.put("zookeeper.session.timeout.ms", "10000");
        props.put("zookeeper.sync.time.ms", "100");
        props.put("auto.commit.interval.ms", "500");
        consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
		
        /**
         * Create multiple streams zu topic: aus consumer 
         */
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        // config streams zu topic
        topicCountMap.put(this.topic, new Integer(this.threads));
        // create streams
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        // get topic aus streams 
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(this.topic);
        
        /**
         * Working with messages aus multiple streams
         * */
        int thread=0;
        for (final KafkaStream stream : streams) {
        	new Thread(new ReadKafkaQueueAndWriteToCassandra(stream,thread,this.toCassandra)).start();   	
 	        thread++;
        }
        
	}
	   
}
