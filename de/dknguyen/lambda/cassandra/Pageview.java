package de.dknguyen.lambda.cassandra;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.CompositeType.Builder;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.thrift.Column;
import org.apache.commons.collections.map.MultiValueMap;

public class Pageview extends Data{
	
	private static final  String pageidColname="pageid";
	private static final  String personidColname="personid";
	private static final  String nonceColname="nonce";
	
	public String pageid;
	public String personid;
	public String nonce;
	
	Pageview(String pedigree, String pageid, String personid, String nonce){
		this.pageid=pageid;
		this.personid=personid;
		this.pedigree=pedigree;
	}
	
	public static ColGroup createColGroup(String pedigree, String pageid, String personid) throws UnsupportedEncodingException{
		
		Long timestamp=System.currentTimeMillis();
	
		List<Column> columns =new ArrayList<Column>();
		
		Column col=new Column();
		/*
		col.setName(ByteBuffer.wrap(perdigreeColname.getBytes("UTF8")));
		col.setValue(ByteBuffer.wrap(pedigree.getBytes("UTF8")));
		col.setTimestamp(timestamp);
		columns.add(col);
		*/
		/*
		List<AbstractType<?>> keyTypes = new ArrayList<AbstractType<?>>(); 
	    keyTypes.add(UTF8Type.instance);
	    keyTypes.add(UTF8Type.instance);
	    CompositeType compositeKey = CompositeType.getInstance(keyTypes);
	    
	    Builder builder = new Builder(compositeKey);
	    builder.add(ByteBuffer.wrap("2".getBytes()));
	    builder.add(ByteBuffer.wrap("value".getBytes()));
	    ByteBuffer columnName = builder.build();
		*/
		
		/*
		col=new Column();
		col.setName(ByteBuffer.wrap(pageidColname.getBytes("UTF8")));
		col.setValue(ByteBuffer.wrap(pageid.getBytes("UTF8")));
		col.setTimestamp(timestamp);
		columns.add(col);
		*/
		
		col=new Column();
		col.setName(ByteBuffer.wrap(pedigree.getBytes("UTF8")));
		col.setValue(ByteBuffer.wrap(personid.getBytes("UTF8")));
		col.setTimestamp(timestamp);
		columns.add(col);
		
		String key=pageid+"/h-"+(Integer.valueOf(pedigree)/3600);
		
		return new ColGroup("pageviews", ByteBuffer.wrap(key.getBytes("UTF8")),columns);
	}

}
