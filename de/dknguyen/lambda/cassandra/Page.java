package de.dknguyen.lambda.cassandra;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Column;

public class Page extends Data{
	
	private static final String urlColname="url";
	private static final String pageviewColname="pageview";
	String url;
	String pageview;
	
	Page(String pedigree, String url, String pageview){
		this.pedigree=pedigree;
		this.url=url;
		this.pageview=pageview;
	}
	
	
	public static ColGroup createColGroup(String pedigree, String url, String pageview) throws UnsupportedEncodingException{
		
		Long timestamp=System.currentTimeMillis();
		List<Column> columns = new ArrayList<Column>();
		
		Column col=new Column();
		col.setName(ByteBuffer.wrap(perdigreeColname.getBytes("UTF8")));
		col.setValue(ByteBuffer.wrap(pedigree.getBytes("UTF8")));
		col.setTimestamp(timestamp);
		columns.add(col);
		
		col=new Column();
		col.setName(ByteBuffer.wrap(urlColname.getBytes("UTF8")));
		col.setValue(ByteBuffer.wrap(url.getBytes("UTF8")));
		col.setTimestamp(timestamp);
		columns.add(col);
		
		
		col=new Column();
		col.setName(ByteBuffer.wrap(pageviewColname.getBytes("UTF8")));
		col.setValue(ByteBuffer.wrap(pageview.getBytes("UTF8")));
		col.setTimestamp(timestamp);
		columns.add(col);
		
		return new ColGroup("pages",ByteBuffer.wrap(url.getBytes("UTF8")),columns);
	}
	
}
