package de.dknguyen.lambda.cassandra;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.thrift.Column;

public class ColGroup {
	public String colFamily;
	public ByteBuffer key;
	public List<Column> colGroup;
	ColGroup(String colFamily,ByteBuffer key, List<Column> colGroup){
		this.colFamily=colFamily;
		this.key=key;
		this.colGroup=colGroup;
	}
	public String getColFamily() {
		return colFamily;
	}
	public void setColFamily(String colFamily) {
		this.colFamily = colFamily;
	}
	public ByteBuffer getKey() {
		return key;
	}
	public void setKey(ByteBuffer key) {
		this.key = key;
	}
	public List<Column> getColGroup() {
		return colGroup;
	}
	public void setColGroup(List<Column> colGroup) {
		this.colGroup = colGroup;
	}
}
