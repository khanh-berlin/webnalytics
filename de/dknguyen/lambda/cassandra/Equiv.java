package de.dknguyen.lambda.cassandra;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Column;
import org.apache.commons.collections.map.MultiValueMap;


public class Equiv extends Data{
	
	private static final String personid1Colname="personid1";
	private static final String personid2Colname="personid2";
	
	public String personid1;
	public String personid2;
	
	Equiv(String pedigree, String personid1, String personid2){
		this.pedigree=pedigree;
		this.personid1=personid1;
		this.personid2=personid2;
	}
	
	
	public static ColGroup createColGroup(String pedigree, String personid1, String personid2) throws UnsupportedEncodingException{
		
		Long timestamp=System.currentTimeMillis();
		ArrayList<Column> columns= new ArrayList<Column>();
		
		
		Column col=new Column();
		/*
		col.setName(ByteBuffer.wrap(perdigreeColname.getBytes("UTF8")));
		col.setValue(ByteBuffer.wrap(pedigree.getBytes("UTF8")));
		col.setTimestamp(timestamp);
		columns.add(col);
		
		
		col=new Column();
		col.setName(ByteBuffer.wrap(personid1Colname.getBytes("UTF8")));
		col.setValue(ByteBuffer.wrap(personid1.getBytes("UTF8")));
		col.setTimestamp(timestamp);
		columns.add(col);
		*/
		
		col=new Column();
		col.setName(ByteBuffer.wrap(personid2.getBytes("UTF8")));
		col.setValue(ByteBuffer.wrap(pedigree.getBytes("UTF8")));
		col.setTimestamp(timestamp);
		columns.add(col);
		
		
		return new ColGroup("equivs",ByteBuffer.wrap(personid1.getBytes("UTF8")), columns);
		
	}
}
