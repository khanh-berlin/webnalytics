package de.dknguyen.lambda;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.backtype.cascading.tap.PailTap;
import com.backtype.hadoop.pail.PailSpec;
import com.google.common.collect.Maps;
import com.twitter.maple.tap.StdoutTap;

import jcascalog.Api;
import jcascalog.Option;
import jcascalog.Subquery;
import jcascalog.op.Count;
import jcascalog.op.Sum;
import cascading.tap.Tap;
import de.dknguyen.lambda.cascalog.EmitGranularities;
import de.dknguyen.lambda.cascalog.NormalizeURL;
import de.dknguyen.lambda.cascalog.ToHourBucket;
import de.dknguyen.lambda.cascalog.ToSerializedLong;
import de.dknguyen.lambda.cascalog.ToUrlBucketedKey;
import de.dknguyen.lambda.cascalog.ExtractPageViewFields;
import de.dknguyen.lambda.pail.SplitDataPailStructure;
import de.dknguyen.lambda.edb.UrlOnlyScheme;
import de.dknguyen.lambda.thrift.DataUnit;
import elephantdb.DomainSpec;
import elephantdb.jcascalog.EDB;
import elephantdb.partition.HashModScheme;
import elephantdb.persistence.JavaBerkDB;

public class BatchLayerPrecomputing {
	
	public static String HDFS;
    
    public static Map<String,String> HADOOP_DIR_STRUCTURE;
    static FileWriter file7;
    
    public static void main(String[] args){
    	queryMasterDataToPageviews();
    }
    
    public static void precomputingMasterdataAndStoreInEDB(String HDFS,Map<String,String> HADOOP_DIR_STRUCTURE) throws IOException{
    	BatchLayerPrecomputing.HDFS=HDFS;
    	BatchLayerPrecomputing.HADOOP_DIR_STRUCTURE=HADOOP_DIR_STRUCTURE;
    	file7 = new FileWriter(System.getenv("LAMBDA_HOME") + "/measure/batch-precomputing.log",true );
    	Date date= new Date();
    	file7.write(date.toString() + "------------------------------\n");
   	    /* ***************************************
  	     * Measure 6 start
  	     * ***************************************/
    	long starttime=System.currentTimeMillis();
    	normalizeUrls();
    	pageviewPrecomputingToEDB();
    	//queryMasterDataToPageviews();
    	long endtime=System.currentTimeMillis();
   	    file7.write((endtime - starttime) + "\n");
   	    file7.close();
   	    /* ***************************************
   	     * Measure 6 end
   	     * ***************************************/
    }
    
    private static void normalizeUrls() {
    	Tap masterDataset = splitDataTap(HDFS+HADOOP_DIR_STRUCTURE.get("DIR_MASTER_DATA"));
    	Tap outTap = splitDataTap(HDFS+HADOOP_DIR_STRUCTURE.get("DIR_NORMALIZED_URLS"));
    	Api.execute(outTap,
                new Subquery("?normalized")
                        .predicate(masterDataset, "_","?raw")
                        .predicate(new NormalizeURL(), "?raw").out("?normalized"));
    }
    
    private static PailTap splitDataTap(String path) {
        PailTap.PailTapOptions opts = new PailTap.PailTapOptions();
        opts.spec = new PailSpec(new SplitDataPailStructure());
        return new PailTap(path, opts);
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
    
    
    
    /*
     * 1. Query Masterdateset
     * 2. Precomputing batchview pageview (url|granuality|buchket|total-views) then store in hdfs
     * 3. Convert pageviews von hdfs in to key/value byte array 
     * 		key=url + "/" + gran + "-" + bucket (z.B: "http://blog.domain_1.org/m-572"
     *      value=total-views
     * 4. Create shards and Store tuple key/value(bytes) in shards (HDFS+"/outputs/edb/pageviews")
     *    you can go this shards folder to see the files.jdb (codierung in JavaBerkDB binary)
     * Note: after creating shards and storing key/values you can start EDB server cluster:
     *        each edb-server will download subset of shards in his local folder and serving this shards
     *        $ java -cp webanalytics.jar elephantdb.keyval.core global_config.clj local_config.clj
     */
    private static void pageviewPrecomputingToEDB(){
    	 //
    	 Tap source= attributeTap(HDFS+HADOOP_DIR_STRUCTURE.get("DIR_NORMALIZED_URLS"),DataUnit._Fields.PAGE_VIEW);
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
          Object edbShard = EDB.makeKeyValTap(HDFS+HADOOP_DIR_STRUCTURE.get("DIR_EDB_PAGEVIEWS"),
        		  new DomainSpec(new JavaBerkDB(),
   				  new HashModScheme(),
   				4));
          //
          Api.execute(edbShard, edbKeyValue);
    }
    
    
    public static void queryMasterDataToPageviews(){
    	
   	   //Tap source= attributeTap(HDFS+HADOOP_DIR_STRUCTURE.get("DIR_MASTER_DATA"), DataUnit._Fields.PAGE_VIEW);
       Tap source= attributeTap(HDFS+HADOOP_DIR_STRUCTURE.get("DIR_NORMALIZED_URLS"), DataUnit._Fields.PAGE_VIEW);
        Subquery hourbucket = new Subquery("?url", "?hour-bucket", "?count")
        		.predicate(source, "_","?pageview")
                .predicate(new ExtractPageViewFields(), "?pageview").out("?url", "_", "?timestamp")
                .predicate(new ToHourBucket(), "?timestamp").out("?hour-bucket")
                .predicate(new Count(), "?count");
   	
        Subquery pageViews = new Subquery("?url", "?granularity", "?bucket", "?total-pageviews")
        .predicate(hourbucket, "?url", "?hour-bucket", "?count")
        .predicate(new EmitGranularities(), "?hour-bucket").out("?granularity", "?bucket")
        .predicate(new Sum(), "?count").out("?total-pageviews");
        //
        Api.execute(new StdoutTap(),pageViews);
    }
}
