package de.dknguyen.lambda.cascalog;

import java.io.UnsupportedEncodingException;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;

public class ToUrlBucketedKey extends CascalogFunction {
	public void operate(FlowProcess process, FunctionCall call) {
		String url = call.getArguments().getString(0);
		String gran = call.getArguments().getString(1);
		Integer bucket = call.getArguments().getInteger(2);
		String keyStr = url + "/" + gran + "-" + bucket;
		
		try {
			call.getOutputCollector().add(new Tuple(keyStr.getBytes("UTF-8")));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}