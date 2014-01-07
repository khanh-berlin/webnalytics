package de.dknguyen.lambda.cassandra;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Column;

public class Person extends Data{
	
	private static final String personidColname="personid";
	private static final String gendertypeColname="gendertype";
	private static final String fullnameColname="fullname";
	private static final String cityColname="city";
	private static final String stateColname="state";
	private static final String countryColname="country";
	
	public String personid;
	public String gendertype;
	public String fullname;
	public String city;
	public String state;
	public String country;
	
	Person(String pedigree,String personid,String gendertype,String fullname,String city,String state,String country){
		this.pedigree=pedigree;
		this.personid=personid;
		this.gendertype=gendertype;
		this.fullname=fullname;
		this.city=city;
		this.state=state;
		this.country=country;
	}

	public static ColGroup createColGroup(String pedigree, String personid, String gendertype, String fullname, String city, String state, String country) throws UnsupportedEncodingException {
		
		Long timestamp=System.currentTimeMillis();
		List<Column> columns = new ArrayList<Column>();
		
		Column col=new Column();
		col.setName(ByteBuffer.wrap(perdigreeColname.getBytes("UTF8")));
		col.setValue(ByteBuffer.wrap(pedigree.getBytes("UTF8")));
		col.setTimestamp(timestamp);
		columns.add(col);
		
		
		col=new Column();
		col.setName(ByteBuffer.wrap(personidColname.getBytes("UTF8")));
		col.setValue(ByteBuffer.wrap(personid.getBytes("UTF8")));
		col.setTimestamp(timestamp);
		columns.add(col);
		
		col=new Column();
		col.setName(ByteBuffer.wrap(gendertypeColname.getBytes("UTF8")));
		col.setValue(ByteBuffer.wrap(gendertype.getBytes("UTF8")));
		col.setTimestamp(timestamp);
		columns.add(col);
		
		col=new Column();
		col.setName(ByteBuffer.wrap(fullnameColname.getBytes("UTF8")));
		col.setValue(ByteBuffer.wrap(fullname.getBytes("UTF8")));
		col.setTimestamp(timestamp);
		columns.add(col);
		
		col=new Column();
		col.setName(ByteBuffer.wrap(cityColname.getBytes("UTF8")));
		col.setValue(ByteBuffer.wrap(city.getBytes("UTF8")));
		col.setTimestamp(timestamp);
		columns.add(col);
		
		col=new Column();
		col.setName(ByteBuffer.wrap(stateColname.getBytes("UTF8")));
		col.setValue(ByteBuffer.wrap(state.getBytes("UTF8")));
		col.setTimestamp(timestamp);
		columns.add(col);
		
		col=new Column();
		col.setName(ByteBuffer.wrap(countryColname.getBytes("UTF8")));
		col.setValue(ByteBuffer.wrap(country.getBytes("UTF8")));
		col.setTimestamp(timestamp);
		columns.add(col);
		
		return new ColGroup("persons",ByteBuffer.wrap(personid.getBytes("UTF8")),columns);
	}

}
