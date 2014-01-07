package de.dknguyen.lambda;

import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import jcascalog.Api;
import jcascalog.Subquery;
import cascalog.ops.IdentityBuffer;
import cascalog.ops.RandLong;

import com.backtype.cascading.tap.PailTap;
import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.PailSpec;
import com.google.common.collect.Maps;

import de.dknguyen.lambda.pail.DataPailStructure;
import de.dknguyen.lambda.pail.SplitDataPailStructure;

public class BatchLayerStorage {
	
	public String HDFS = "hdfs://b-sso-02d:54310";
   
    
    public Map<String,String> HADOOP_DIR_STRUCTURE;
    FileWriter file6;
    
    public BatchLayerStorage(String HDFS, Map<String,String> HADOOP_DIR_STRUCTURE){
    	
    	this.HDFS=HDFS;
    	this.HADOOP_DIR_STRUCTURE=HADOOP_DIR_STRUCTURE;
    }
    
    public void insertNewDataIntoMaster() throws IOException, URISyntaxException{
    	configPail();
    	file6 = new FileWriter(System.getenv("LAMBDA_HOME") + "/measure/batch-storage.log",true );
    	Date date= new Date();
    	file6.write(date.toString() + "------------------------------\n");
    	 /* ***************************************
   	     * Measure 6 start
   	     * ***************************************/
    	Long starttime=System.currentTimeMillis();
    	Pail newDataPail= new Pail(HDFS+HADOOP_DIR_STRUCTURE.get("DIR_NEW_DATA"));
    	Pail masterPail = new Pail(HDFS+HADOOP_DIR_STRUCTURE.get("DIR_MASTER_DATA"));
   	    Pail snapshotPail = newDataPail.snapshot(HDFS+HADOOP_DIR_STRUCTURE.get("DIR_SNAPSHOT"));
   	    shredNewData(HDFS+HADOOP_DIR_STRUCTURE.get("DIR_SNAPSHOT"), HDFS+HADOOP_DIR_STRUCTURE.get("DIR_SHREDDED_DATA"));
   	    consolidateAndAbsorb(masterPail, new Pail(HDFS+HADOOP_DIR_STRUCTURE.get("DIR_SHREDDED_DATA")));
   	    newDataPail.deleteSnapshot(snapshotPail);
   	    Long endtime=System.currentTimeMillis();
   	    file6.write((endtime - starttime) + "\n");
   	    file6.close();
   	    /* ***************************************
   	     * Measure 6 end
   	     * ***************************************/
   	    
    }
    
    private static void consolidateAndAbsorb(Pail masterPail, Pail shreddedPail) throws IOException {
        shreddedPail.consolidate();
        masterPail.absorb(shreddedPail);
    }
    
    private static void shredNewData(String snapshotLocation, String shreddedDataLocation) {
    	PailTap source = deserializeDataTap(snapshotLocation); 	
    	PailTap sink = splitDataTap(shreddedDataLocation);
        Subquery reduced = new Subquery("?rand", "?data")
                .predicate(source, "_", "?data-in")
                .predicate(new RandLong(), "?rand")
                .predicate(new IdentityBuffer(), "?data-in").out("?data");

        Api.execute(
                sink,
                new Subquery("?data")
                        .predicate(reduced, "_", "?data"));
    }
    
    public static PailTap splitDataTap(String path) {
        PailTap.PailTapOptions opts = new PailTap.PailTapOptions();
        opts.spec = new PailSpec(new SplitDataPailStructure());
        return new PailTap(path, opts);
    }
    
    public static PailTap deserializeDataTap(String path) {
        PailTap.PailTapOptions opts = new PailTap.PailTapOptions();
        opts.spec = new PailSpec(new DataPailStructure());
        return new PailTap(path, opts);
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
}
