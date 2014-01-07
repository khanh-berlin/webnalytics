package de.dknguyen.lambda.edb;

import java.io.UnsupportedEncodingException;
import elephantdb.partition.ShardingScheme;

public class UrlOnlyScheme implements ShardingScheme {
	public int shardIndex(byte[] shardKey, int shardCount) {
		String url = getUrlFromSerializedKey(shardKey);
		return url.hashCode() % shardCount;
	}
	private static String getUrlFromSerializedKey(byte[] ser) {
 		try {
			String key = new String(ser, "UTF-8");
			return key.substring(0, key.lastIndexOf("/"));
		} catch(UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}
}
